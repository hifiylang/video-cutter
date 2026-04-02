from pathlib import Path

from dotenv import load_dotenv

from .base import BaseConfig


def get_config():
    # 中文注释：统一从项目根目录 .env 加载配置，覆盖旧的硬编码环境区分逻辑。
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    return BaseConfig()

