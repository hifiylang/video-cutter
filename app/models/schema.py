from typing import List, Optional
from pydantic import BaseModel


class VideoJobInput(BaseModel):
    job_id: str
    source_url: str
    ext: str = None
    prefix: Optional[str] = None
    http_headers: Optional[dict] = None
    container: str = "mp4"  # target container: mp4 or avi
    copy_first: bool = True  # try codec copy before transcode


class ChunkInfo(BaseModel):
    index: int
    size: int
    # object_key: str
    url: Optional[str] = None
    # 00:00:00   00:03:43
    start_time:str
    end_time:str


class VideoJobOutput(BaseModel):
    job_id: str
    # source_url: str
    total_size: int
    # chunk_size: int
    chunks: List[ChunkInfo]
    ext: str
    error: Optional[str] = None
    finished: bool = False
