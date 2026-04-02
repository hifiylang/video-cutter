import os
import shutil
import subprocess
import unittest
import shutil as _shutil

from app.services.video.chunker import split_file


class TestChunker(unittest.TestCase):
    def setUp(self):
        self.tmpdir = "tmp_test_chunker"
        os.makedirs(self.tmpdir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_ffmpeg_split_under_30m(self):
        if _shutil.which("ffmpeg") is None:
            self.skipTest("ffmpeg not available")
        path = os.path.join(self.tmpdir, "sample.mp4")
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
            "-f", "lavfi", "-i", "sine=frequency=1000",
            "-t", "12",
            "-c:v", "libx264", "-preset", "veryfast", "-b:v", "2000k",
            "-c:a", "aac", "-b:a", "128k",
            path,
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        out_dir = os.path.join(self.tmpdir, "chunks")
        parts = split_file(path, 30 * 1024 * 1024, out_dir)
        self.assertGreaterEqual(len(parts), 1)
        for part in parts:
            self.assertTrue(part["path"].endswith(".mp4"))
            self.assertLessEqual(part["size"], 30 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
