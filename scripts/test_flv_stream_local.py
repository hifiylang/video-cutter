"""Local helper to exercise the FLV stream monitor without Kafka/TOS.

Before running, fill in FLV_URL with a reachable flv stream.

It will:
- Run the FLV streamer in-process (no multiprocessing)
- Write chunks to a temp folder
- Print each chunk's size
- Verify each chunk is playable via ffprobe

Run:
    python scripts/test_flv_stream_local.py
"""

import os
import subprocess
import tempfile
import uuid

from app.models.schema import VideoJobInput
from app.services.video.flv_streamer import FlvStreamProcessor, TARGET_CHUNK_BYTES



FLV_URL = "http://pull-x2-f5.douyincdn.com/stage/stream-694983619624043178_or4.flv?arch_hrchy=c1&exp_hrchy=c1&expire=1766733168&major_anchor_level=common&rtm_expr_tag=reflow_room_info&sign=28c0ff43e04f63dbc8de8a7af7872b79&t_id=037-20251219151248A02DD077ECFBE45B999B-KRR4Pj&unique_id=stream-694983619624043178_682_flv_or4&volcSecret=28c0ff43e04f63dbc8de8a7af7872b79&volcTime=1766733168"
HTTP_HEADERS = {
    # "User-Agent": "Mozilla/5.0",
    # "Referer": "https://example.com/",
}


def _ffprobe_ok(path: str) -> bool:
    try:
        p = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nokey=1:noprint_wrappers=1",
                path,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return bool((p.stdout or "").strip())
    except Exception:
        return False


def main():
    if not FLV_URL:
        raise SystemExit("Please set FLV_URL to a valid flv stream URL before running.")

    job_id = uuid.uuid4().hex

    # ext must be passed through as-is for downstream; container controls the output suffix.
    job = VideoJobInput(job_id=job_id, source_url=FLV_URL, ext="flv", prefix="localFlvTest", http_headers=HTTP_HEADERS, container="mp4")

    with tempfile.TemporaryDirectory(prefix="flv-smoke-") as td:
        base_dir = os.path.join(td, job_id)
        chunk_dir = os.path.join(base_dir, "chunks")

        p = FlvStreamProcessor(job, http_headers=HTTP_HEADERS, container="mp4", max_segment_bytes=TARGET_CHUNK_BYTES)

        # redirect output dirs to temp
        p.base_dir = base_dir
        p.chunk_dir = chunk_dir
        p.internal_dir = os.path.join(base_dir, "internal")
        p.internal_out_pattern = os.path.join(p.internal_dir, "seg-%08d.mp4")
        p.internal_glob_pattern = os.path.join(p.internal_dir, "seg-*.mp4")
        p.out_pattern = os.path.join(chunk_dir, "part-%05d.mp4")
        p.glob_pattern = os.path.join(chunk_dir, "part-*.mp4")

        # disable kafka + tos for local test
        p.producer = None

        class _NoTOS:
            def upload_file(self, object_key: str, path: str):
                return {"status_code": 200, "request_id": "local"}

            def build_public_url(self, object_key: str):
                return f"file://{object_key}"

        p.tos = _NoTOS()  # type: ignore

        produced = []

        def _handle_completed_file_local(path: str, index: int):
            size = os.path.getsize(path)
            ok = _ffprobe_ok(path)
            produced.append((index, size, ok, path))
            print(f"chunk index={index:05d} size={size/1024/1024:.2f}MB playable={ok} path={path}")

        p._handle_completed_file = _handle_completed_file_local  # type: ignore

        p.run(FLV_URL)

        if not produced:
            raise SystemExit("No chunks produced. Stream may have ended too quickly or URL is invalid.")

        bad = [x for x in produced if not x[2]]
        if bad:
            raise SystemExit(f"Some chunks are not playable via ffprobe: {bad[:3]}")

        overs = [x for x in produced if x[1] > TARGET_CHUNK_BYTES]
        if overs:
            raise SystemExit(f"Some chunks exceed 30MB cap: {overs[:3]}")

        # Keep temp directory path in output for debugging.
        print(f"done. temp_dir={td}")


if __name__ == "__main__":
    main()
