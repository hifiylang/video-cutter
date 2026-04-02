import os
import shutil
import subprocess
import glob
from typing import Optional


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _probe_duration_seconds(file_path: str) -> float:
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=nokey=1:noprint_wrappers=1", file_path
        ], check=True, capture_output=True, text=True)
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def split_file(file_path: str, chunk_size: int, out_dir: str):
    ensure_dir(out_dir)
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg is required for video segmentation")
    for p in glob.glob(os.path.join(out_dir, "part-*.mp4")):
        try:
            os.remove(p)
        except Exception:
            pass
    duration = _probe_duration_seconds(file_path)
    size_bytes = os.path.getsize(file_path)
    avg_total_kbps = 0
    if duration and duration > 0:
        avg_total_kbps = int((size_bytes * 8) / (duration * 1000))
    a_kbps = 128
    v_kbps = max(300, (avg_total_kbps - a_kbps) if avg_total_kbps > a_kbps else 2000)
    out_pattern = os.path.join(out_dir, "part-%02d.mp4")
    def calc_seg_time(v: int, a: int):
        total = v + a
        t = int((chunk_size * 8 * 0.90) / (total * 1000))
        return 1 if t <= 0 else t
    def build_cmd(encoder: str, v: int, a: int, seg_t: int):
        return [
            "ffmpeg",
            "-y",
            "-i",
            file_path,
            "-map",
            "0",
            "-c:v",
            encoder,
            "-b:v",
            f"{v}k",
            "-minrate",
            f"{v}k",
            "-maxrate",
            f"{v}k",
            "-bufsize",
            f"{v}k",
            "-sc_threshold",
            "0",
            "-force_key_frames",
            f"expr:gte(t,n_forced*{seg_t})",
            "-c:a",
            "aac",
            "-b:a",
            f"{a}k",
            "-f",
            "segment",
            "-segment_time",
            str(seg_t),
            "-reset_timestamps",
            "1",
            out_pattern,
        ]

    def build_cmd_part(src: str, encoder: str, v: int, a: int, seg_t: int, out_pat: str):
        return [
            "ffmpeg",
            "-y",
            "-i",
            src,
            "-map",
            "0",
            "-c:v",
            encoder,
            "-b:v",
            f"{v}k",
            "-minrate",
            f"{v}k",
            "-maxrate",
            f"{v}k",
            "-bufsize",
            f"{v}k",
            "-sc_threshold",
            "0",
            "-force_key_frames",
            f"expr:gte(t,n_forced*{seg_t})",
            "-c:a",
            "aac",
            "-b:a",
            f"{a}k",
            "-f",
            "segment",
            "-segment_time",
            str(seg_t),
            "-reset_timestamps",
            "1",
            out_pat,
        ]

    def build_cmd_two(src: str, encoder: str, v: int, a: int, ss: Optional[int], t: Optional[int], out_file: str):
        cmd = [
            "ffmpeg",
            "-y",
        ]
        if ss is not None:
            cmd += ["-ss", str(ss)]
        cmd += [
            "-i",
            src,
            "-map",
            "0",
            "-c:v",
            encoder,
            "-b:v",
            f"{v}k",
            "-minrate",
            f"{v}k",
            "-maxrate",
            f"{v}k",
            "-bufsize",
            f"{v}k",
            "-c:a",
            "aac",
            "-b:a",
            f"{a}k",
        ]
        if t is not None:
            cmd += ["-t", str(t)]
        cmd += [
            "-movflags",
            "+faststart",
            out_file,
        ]
        return cmd

    seg_time = calc_seg_time(v_kbps, a_kbps)
    used_encoder = "libx264"
    proc = subprocess.run(build_cmd(used_encoder, v_kbps, a_kbps, seg_time), check=False, capture_output=True)
    if proc.returncode != 0:
        for enc in ("h264_videotoolbox", "libopenh264"):
            used_encoder = enc
            proc = subprocess.run(build_cmd(enc, v_kbps, a_kbps, seg_time), check=False, capture_output=True)
            if proc.returncode == 0:
                break
        else:
            raise RuntimeError((proc.stderr or proc.stdout).decode("utf-8", errors="ignore"))
    files = sorted(glob.glob(os.path.join(out_dir, "part-*.mp4")))

    def split_part_recursive(src_path: str, max_bytes: int, depth: int = 0, max_depth: int = 6):
        sz = os.path.getsize(src_path)
        if sz <= max_bytes or depth >= max_depth:
            return [src_path]
        dur = _probe_duration_seconds(src_path)
        half = max(1, int(dur / 2))
        base = os.path.splitext(os.path.basename(src_path))[0]
        first_out = os.path.join(out_dir, f"{base}-01.mp4")
        second_out = os.path.join(out_dir, f"{base}-02.mp4")
        for pth in glob.glob(os.path.join(out_dir, f"{base}-*.mp4")):
            try:
                os.remove(pth)
            except Exception:
                pass
        p1 = subprocess.run(build_cmd_two(src_path, used_encoder, v_kbps, a_kbps, None, half, first_out), check=False, capture_output=True)
        if p1.returncode != 0:
            raise RuntimeError((p1.stderr or p1.stdout).decode("utf-8", errors="ignore"))
        p2 = subprocess.run(build_cmd_two(src_path, used_encoder, v_kbps, a_kbps, half, None, second_out), check=False, capture_output=True)
        if p2.returncode != 0:
            raise RuntimeError((p2.stderr or p2.stdout).decode("utf-8", errors="ignore"))
        try:
            os.remove(src_path)
        except Exception:
            pass
        children = [first_out, second_out]
        result = []
        for ch in children:
            result.extend(split_part_recursive(ch, max_bytes, depth + 1, max_depth))
        return result

    final_files = []
    for p in files:
        if os.path.getsize(p) <= chunk_size:
            final_files.append(p)
        else:
            final_files.extend(split_part_recursive(p, chunk_size))
    def _format_hms(seconds: float) -> str:
        s = int(seconds)
        h = s // 3600
        m = (s % 3600) // 60
        sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"
    parts = []
    current_start = 0.0
    for i, p in enumerate(final_files, start=1):
        dur = _probe_duration_seconds(p)
        start_str = _format_hms(current_start)
        end_str = _format_hms(current_start + dur)
        parts.append({
            "path": p,
            "size": os.path.getsize(p),
            "index": i,
            "start_time": start_str,
            "end_time": end_str,
        })
        current_start += dur
    return parts
