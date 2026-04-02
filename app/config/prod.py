from .base import BaseConfig


class ProdConfig(BaseConfig):
    # 中文注释：保留旧类名，避免其他模块导入时报错，实际配置统一走 .env。
    pass
