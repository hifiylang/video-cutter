import glob
import os
import shutil
import subprocess
import time
from multiprocessing import Queue
from typing import Optional

from app.config import get_config
from app.models.schema import ChunkInfo, VideoJobInput, VideoJobOutput
from app.services.kafka.producer import Producer
from app.services.tos.client import TOSClient
from app.utils.logging import get_logger


def _probe_duration_seconds(file_path: str) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nokey=1:noprint_wrappers=1",
                file_path,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def _format_hms(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


# 尝试获取码率，取不到就返回 2500kbps
def _approx_bitrate_kbps(url: str, http_headers: Optional[dict] = None, timeout_sec: int = 5) -> int:
    """Best-effort estimate of input bitrate (kbps).

    For live FLV streams we may not have a duration. We try ffprobe first; if it fails,
    fall back to a conservative default to keep segments under the size target.
    """
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=bit_rate",
            "-of",
            "default=nokey=1:noprint_wrappers=1",
        ]
        if http_headers:
            header_lines = [f"{k}: {v}" for k, v in http_headers.items()]
            cmd += ["-headers", "\r\n".join(header_lines) + "\r\n"]
        cmd.append(url)
        result = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout_sec)
        s = (result.stdout or "").strip()
        if s.isdigit():
            bps = int(s)
            if bps > 0:
                return max(300, int(bps / 1000))
    except Exception:
        pass

    # Fallback: assume a reasonably high bitrate so segments are shorter (safer for <30MB).
    return 2500


# 根据码率、大小  反推 时间
def _segment_seconds_for_max_bytes(max_bytes: int, bitrate_kbps: int, safety: float = 0.85) -> int:
    """Convert desired max bytes + bitrate to an ffmpeg segment_time.

    size(bytes) ≈ bitrate(kbps) * 1000/8 * seconds
    => seconds ≈ max_bytes * 8 / (bitrate_kbps * 1000)

    safety<1 leaves headroom for muxing overhead and bitrate spikes.
    """
    bitrate_kbps = max(1, int(bitrate_kbps))
    sec = int((max_bytes * 8 * safety) / (bitrate_kbps * 1000))
    return max(2, sec)


# 对象归一处理
def _normalize_job_payload(job_payload: dict) -> dict:
    """Support both snake_case (existing) and camelCase (new message) keys."""
    if not isinstance(job_payload, dict):
        raise TypeError("job_payload must be a dict")

    payload = dict(job_payload)

    # Accept incoming format: {jobId, sourceUrl, ext:{...}, prefix:null}
    if "job_id" not in payload and "jobId" in payload:
        payload["job_id"] = str(payload.get("jobId"))
    if "source_url" not in payload and "sourceUrl" in payload:
        payload["source_url"] = payload.get("sourceUrl")

    # align optional fields
    if payload.get("prefix", None) is None:
        payload["prefix"] = None

    return payload


# 30MB target (hard cap per emitted file)
TARGET_CHUNK_BYTES = 30 * 1024 * 1024
# Internal sub-segment duration for stable concat remuxing (seconds)
INTERNAL_SEGMENT_SECONDS = 2


class FlvStreamProcessor:
    def __init__(
        self,
        job: VideoJobInput,
        http_headers: Optional[dict] = None,
        container: str = "mp4",
        max_segment_bytes: int = TARGET_CHUNK_BYTES,
    ):
        self.job = job
        self.cfg = get_config()
        self.log = get_logger("flv-stream")
        self.base_dir = os.path.join(self.cfg.data_dir, job.job_id)
        self.chunk_dir = os.path.join(self.base_dir, "chunks")
        self.max_segment_bytes = int(max_segment_bytes)
        self.http_headers = http_headers or {}
        self.prefix = job.prefix or "videoCutter"
        self.total_size = 0
        self.current_start_seconds = 0.0

        # Producer is optional; if it fails to init we'll just log and keep processing.
        self.producer: Optional[Producer] = None
        try:
            self.producer = Producer()
        except Exception as e:
            self.log.error("producer init failed, kafka send disabled job_id=%s error=%s", job.job_id, e)

        self.tos = TOSClient()
        self.output_topic = self.cfg.kafka_output_topic
        self.container = (container or "mp4").lower()
        if self.container not in {"mp4", "avi"}:
            self.container = "mp4"

        # Internal short segments are written here, then accumulated into final outputs.
        self.internal_dir = os.path.join(self.base_dir, "internal")
        self.internal_out_pattern = os.path.join(self.internal_dir, f"seg-%05d.{self.container}")
        self.internal_glob_pattern = os.path.join(self.internal_dir, f"seg-*.{self.container}")

        # Final output files (<= max_segment_bytes)
        self.out_pattern = os.path.join(self.chunk_dir, f"part-%05d.{self.container}")
        self.glob_pattern = os.path.join(self.chunk_dir, f"part-*.{self.container}")

        # Internal segmentation cadence is fixed; final size is controlled by accumulation.
        self.segment_seconds: int = INTERNAL_SEGMENT_SECONDS

        # Dedup internal segment file paths
        self._processed_files: set[str] = set()

        # Accumulator state
        self._pending_paths: list[str] = []
        self._pending_bytes: int = 0

        # Final chunk index
        self._next_index: int = 1

        # Flush is synchronous now; no background threads/locks.
        self._flush_inflight = False

    def run(self, source_url: str):
        if shutil.which("ffmpeg") is None:
            raise RuntimeError("ffmpeg is required for flv stream processing")
        os.makedirs(self.chunk_dir, exist_ok=True)
        os.makedirs(self.internal_dir, exist_ok=True)

        target_total_kbps = max(800, _approx_bitrate_kbps(source_url, http_headers=self.http_headers))

        error: Optional[str] = None
        try:
            self._run_ffmpeg_transcoded(source_url, total_kbps=target_total_kbps)
        except Exception as e:
            error = str(e)
            self.log.error("ffmpeg/stream processing failed job_id=%s error=%s", self.job.job_id, error, exc_info=True)
            raise
        finally:
            # Flush remaining pending segments into a final chunk (must still respect <= max_segment_bytes).
            try:
                self._flush_pending(force=True)
            except Exception as e:
                self.log.error("flush pending failed job_id=%s error=%s", self.job.job_id, e, exc_info=True)

            # Send final result BEFORE closing producer.
            # ONLY here (or in global error handler) do we set finished=True to signal end of stream.
            try:
                final_out = VideoJobOutput(
                    job_id=self.job.job_id,
                    total_size=self.total_size,
                    chunks=[],
                    ext=self.job.ext,
                    error=error,
                    finished=True,
                )
                if self.producer:
                    self.producer.send(final_out.model_dump(), topic=self.output_topic)
            except Exception as send_err:
                self.log.error(
                    "failed to send final result job_id=%s error=%s", self.job.job_id, send_err, exc_info=True
                )

            # Close producer last.
            try:
                if self.producer:
                    self.producer.close()
            except Exception:
                pass

            # delete per-job directory when monitoring completes (best-effort)
            try:
                shutil.rmtree(self.base_dir)
            except Exception as e:
                self.log.error("cleanup failed job_id=%s path=%s error=%s", self.job.job_id, self.base_dir, e)

    def _run_ffmpeg_transcoded(self, source_url: str, total_kbps: int):
        a_kbps = 128
        v_kbps = max(400, int(total_kbps) - a_kbps)

        cmd = [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-rw_timeout",
            "15000000",
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_delay_max",
            "30",
        ]
        if self.http_headers:
            header_lines = [f"{k}: {v}" for k, v in self.http_headers.items()]
            cmd += ["-headers", "\r\n".join(header_lines) + "\r\n"]
        cmd += ["-i", source_url]

        if self.container == "mp4":
            cmd += [
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-b:v",
                f"{v_kbps}k",
                "-maxrate",
                f"{v_kbps}k",
                "-bufsize",
                f"{v_kbps}k",
                "-g",
                str(self.segment_seconds * 2),
                "-keyint_min",
                str(self.segment_seconds * 2),
                "-sc_threshold",
                "0",
                "-force_key_frames",
                f"expr:gte(t,n_forced*{self.segment_seconds})",
                "-c:a",
                "aac",
                "-b:a",
                f"{a_kbps}k",
                "-f",
                "segment",
                "-segment_time",
                str(self.segment_seconds),
                "-reset_timestamps",
                "1",
                "-segment_format",
                "mp4",
                "-movflags",
                "+faststart",
                self.internal_out_pattern,
            ]
        else:
            cmd += [
                "-c:v",
                "mpeg4",
                "-qscale:v",
                "5",
                "-c:a",
                "mp3",
                "-f",
                "segment",
                "-segment_time",
                str(self.segment_seconds),
                "-reset_timestamps",
                "1",
                self.internal_out_pattern,
            ]

        self.log.info(
            "starting flv stream capture job_id=%s url=%s internal_segment=%ss target_kbps=%s out=%s",
            self.job.job_id,
            source_url,
            self.segment_seconds,
            total_kbps,
            self.internal_out_pattern,
        )

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self._monitor_segments(proc)
            ret = proc.wait()
            if ret != 0:
                stderr = (proc.stderr.read() if proc.stderr else b"").decode("utf-8", errors="ignore")
                raise RuntimeError(f"ffmpeg exited with code {ret}: {stderr}")
        finally:
            if proc.poll() is None:
                proc.kill()

    def _monitor_segments(self, proc: subprocess.Popen):
        while True:
            self._process_ready_files(force=False)
            # flush synchronously when reaching cap
            if self._pending_bytes >= self.max_segment_bytes:
                self._flush_pending(force=False)

            if proc.poll() is not None:
                self._process_ready_files(force=True)
                self._flush_pending(force=True)
                break
            time.sleep(0.5)

    def _process_ready_files(self, force: bool):
        files = sorted(glob.glob(self.internal_glob_pattern))
        
        # If not forced, exclude the last file (currently being written)
        # to ensure we only process stable, completed segments.
        candidates = files
        if not force and files:
            candidates = files[:-1]

        for path in candidates:
            if path in self._processed_files:
                continue

            if not os.path.exists(path):
                continue

            try:
                self._enqueue_internal_segment(path)
                self._processed_files.add(path)
            except Exception as e:
                self.log.error(
                    "process internal segment failed job_id=%s path=%s error=%s", self.job.job_id, path, e, exc_info=True
                )

    def _enqueue_internal_segment(self, path: str):
        """Add an internal small segment to the accumulator, emitting final chunks when threshold reached.

        Hard rule: each emitted chunk size must be <= max_segment_bytes.
        """
        sz = os.path.getsize(path)
        self._pending_paths.append(path)
        self._pending_bytes += sz

        self.log.debug(
            "queued internal seg job_id=%s path=%s bytes=%s pending_files=%s pending_bytes=%s",
            self.job.job_id,
            path,
            sz,
            len(self._pending_paths),
            self._pending_bytes,
        )

    def _flush_pending(self, force: bool):
        """Emit one final chunk from pending segments.

        This method may take time (concat+upload). It is executed in a background thread.
        """
        if not self._pending_paths:
            return
        if not force and self._pending_bytes < self.max_segment_bytes:
            return

        consumed_paths = list(self._pending_paths)
        self.log.info(
            "flush start job_id=%s files=%s bytes=%s force=%s",
            self.job.job_id,
            len(consumed_paths),
            self._pending_bytes,
            force,
        )

        leftover_paths: list[str] = []

        while True:
            out_path = self.out_pattern % self._next_index
            self.log.info("concat start job_id=%s out=%s inputs=%s", self.job.job_id, out_path, len(consumed_paths))
            self._concat_segments(consumed_paths, out_path)
            out_size = os.path.getsize(out_path)
            self.log.info("concat done job_id=%s out=%s bytes=%s", self.job.job_id, out_path, out_size)

            if out_size <= self.max_segment_bytes:
                break

            if len(consumed_paths) <= 1:
                raise RuntimeError(f"cannot satisfy max size cap, out_size={out_size} cap={self.max_segment_bytes}")

            last = consumed_paths.pop()
            leftover_paths.insert(0, last)
            try:
                os.remove(out_path)
            except Exception:
                pass

        idx = self._next_index
        self.log.info("handle chunk start job_id=%s index=%s", self.job.job_id, idx)
        self._handle_completed_file(out_path, idx)
        self.log.info("handle chunk done job_id=%s index=%s", self.job.job_id, idx)

        # On success, delete consumed internal segments.
        for p in consumed_paths:
            try:
                os.remove(p)
            except Exception:
                pass

        # Update pending state under lock.
        self._next_index += 1
        if leftover_paths:
            self._pending_paths = leftover_paths
            self._pending_bytes = sum(os.path.getsize(p) for p in leftover_paths if os.path.exists(p))
        else:
            self._pending_paths = []
            self._pending_bytes = 0

    def _concat_segments(self, paths: list[str], out_path: str):
        """Concatenate internal mp4 segments into a single mp4 output (fast, copy).

        Requires internal segments to share the same codecs/params (we ensured by forcing transcode).
        """
        os.makedirs(self.internal_dir, exist_ok=True)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        list_path = os.path.join(self.internal_dir, f"concat-{int(time.time()*1000)}.txt")

        def _escape_concat_path(p: str) -> str:
            # ffmpeg concat demuxer: each line is: file '<path>'
            # Escape single quotes by closing/opening quotes.
            return p.replace("'", "'\\''")

        try:
            with open(list_path, "w", encoding="utf-8") as f:
                for p in paths:
                    abs_p = os.path.abspath(p)
                    f.write(f"file '{_escape_concat_path(abs_p)}'\n")

            cmd = [
                "ffmpeg",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                os.path.abspath(list_path),
                "-c",
                "copy",
            ]
            if self.container == "mp4":
                cmd += ["-movflags", "+faststart"]
            cmd.append(os.path.abspath(out_path))

            # Important: use cwd='/' so relative paths in any debug output don't confuse,
            # and to avoid internal_dir being prefixed twice by some ffmpeg builds.
            proc = subprocess.run(cmd, check=False, capture_output=True, cwd="/", timeout=120)
            if proc.returncode != 0:
                raise RuntimeError((proc.stderr or proc.stdout).decode("utf-8", errors="ignore"))
        finally:
            try:
                os.remove(list_path)
            except Exception:
                pass

    def _handle_completed_file(self, path: str, index: int):
        duration = _probe_duration_seconds(path)
        if duration <= 0:
            duration = float(self.segment_seconds)
        start_time = _format_hms(self.current_start_seconds)
        end_time = _format_hms(self.current_start_seconds + duration)
        self.current_start_seconds += duration
        size = os.path.getsize(path)

        object_key = f"{self.prefix}/{self.job.job_id}/part-{index:05d}.{self.container}"
        res = self.tos.upload_file(object_key, path)
        self.log.info(
            "uploaded flv chunk job_id=%s index=%s size=%s key=%s status=%s request_id=%s",
            self.job.job_id,
            index,
            size,
            object_key,
            res.get("status_code"),
            res.get("request_id"),
        )
        url = self.tos.build_public_url(object_key)

        chunk = ChunkInfo(index=index, size=size, url=url, start_time=start_time, end_time=end_time)
        self.total_size += size
        # Explicitly mark finished=False for all intermediate chunks
        payload = VideoJobOutput(
            job_id=self.job.job_id,
            total_size=self.total_size,
            chunks=[chunk],
            ext=self.job.ext,
            finished=False,
        )

        if self.producer:
            self.producer.send(payload.model_dump(), topic=self.output_topic)

        try:
            os.remove(path)
        except Exception:
            pass


def run_flv_process(job_payload: dict, status_queue: Optional[Queue] = None):
    log = get_logger("flv-stream")

    normalized = _normalize_job_payload(job_payload)
    job_id_for_log = normalized.get("job_id") or normalized.get("jobId")

    try:
        job = VideoJobInput(**normalized)
        source_url = normalized.get("source_url") or job.source_url
        container = (normalized.get("container") or "mp4").lower()
        http_headers = normalized.get("http_headers") or {}

        processor = FlvStreamProcessor(
            job,
            http_headers=http_headers,
            container=container,
            max_segment_bytes=TARGET_CHUNK_BYTES,
        )
        processor.run(source_url)

        if status_queue:
            status_queue.put({"status": "ok"})
        return

    except Exception as e:
        err = str(e)
        log.error("flv stream processing failed job_id=%s error=%s", job_id_for_log, err, exc_info=True)

        # Best-effort error result if processor couldn't send (e.g., producer init failed)
        try:
            cfg = get_config()
            out = VideoJobOutput(
                job_id=str(job_id_for_log or ""),
                total_size=0,
                chunks=[],
                ext=normalized.get("ext") or "mp4",
                error=err,
                finished=True,
            )
            prod = Producer()
            prod.send(out.model_dump(), topic=cfg.kafka_output_topic)
            prod.close()
        except Exception:
            pass

        if status_queue:
            status_queue.put({"status": "error", "error": err})
        raise

