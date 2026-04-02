import os
from io import BytesIO
from typing import Optional

import tos

from app.config import get_config


class TOSClient:
    def __init__(self):
        cfg = get_config()
        self.bucket = cfg.tos_bucket
        self.tos_public_host = cfg.tos_public_host
        self.client = tos.TosClientV2(cfg.tos_access_key, cfg.tos_secret_key, cfg.tos_endpoint, cfg.tos_region,
                                      socket_timeout=1800)

    def upload_file(self, object_key: str, file_path: str) -> dict:
        with open(file_path, "rb") as f:
            result = self.client.put_object(self.bucket, object_key, content=f)
        return {
            "status_code": result.status_code,
            "request_id": result.request_id,
            "crc64": getattr(result, "hash_crc64_ecma", None),
        }

    def build_public_url(self, object_key: str) -> Optional[str]:
        if not self.tos_public_host:
            return None
        return f"{self.tos_public_host}/{object_key}"
