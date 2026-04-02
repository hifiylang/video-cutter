import os
from pydantic import BaseModel, Field


def _get_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value


def _get_env_list(name: str) -> list[str]:
    raw_value = _get_env(name)
    if not raw_value:
        return []
    # 中文注释：统一用逗号分隔多地址配置，方便在单个 .env 文件中维护。
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _get_env_bool(name: str, default: bool = False) -> bool:
    raw_value = _get_env(name, str(default)).strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


class BaseConfig(BaseModel):
    # 中文注释：所有外部依赖配置都从 .env / 环境变量读取，代码中不再保留真实地址或密钥。
    kafka_bootstrap_servers: list[str] = Field(default_factory=lambda: _get_env_list("KAFKA_BOOTSTRAP_SERVERS"))
    kafka_input_topic: str = Field(default_factory=lambda: _get_env("KAFKA_INPUT_TOPIC"))
    kafka_flv_topic: str = Field(default_factory=lambda: _get_env("KAFKA_FLV_TOPIC"))
    kafka_output_topic: str = Field(default_factory=lambda: _get_env("KAFKA_OUTPUT_TOPIC"))
    kafka_group_id: str = Field(default_factory=lambda: _get_env("KAFKA_GROUP_ID"))
    kafka_flv_group_id: str = Field(default_factory=lambda: _get_env("KAFKA_FLV_GROUP_ID"))

    tos_access_key: str = Field(default_factory=lambda: _get_env("TOS_ACCESS_KEY"))
    tos_secret_key: str = Field(default_factory=lambda: _get_env("TOS_SECRET_KEY"))
    tos_endpoint: str = Field(default_factory=lambda: _get_env("TOS_ENDPOINT"))
    tos_region: str = Field(default_factory=lambda: _get_env("TOS_REGION"))
    tos_bucket: str = Field(default_factory=lambda: _get_env("TOS_BUCKET"))
    tos_public_host: str = Field(default_factory=lambda: _get_env("TOS_PUBLIC_HOST"))

    data_dir: str = Field(default_factory=lambda: _get_env("DATA_DIR", "data"))
    log_level: str = Field(default_factory=lambda: _get_env("LOG_LEVEL", "INFO"))
    use_presigned_url: bool = Field(default_factory=lambda: _get_env_bool("USE_PRESIGNED_URL", False))
