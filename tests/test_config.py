import os
import unittest
from pathlib import Path

from app.config import get_config
from app.config.base import BaseConfig


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.env_path = Path(".env")
        self.original_env_content = self.env_path.read_text(encoding="utf-8") if self.env_path.exists() else None
        self.original_values = {
            "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
            "KAFKA_INPUT_TOPIC": os.environ.get("KAFKA_INPUT_TOPIC"),
            "KAFKA_FLV_TOPIC": os.environ.get("KAFKA_FLV_TOPIC"),
            "KAFKA_OUTPUT_TOPIC": os.environ.get("KAFKA_OUTPUT_TOPIC"),
            "KAFKA_GROUP_ID": os.environ.get("KAFKA_GROUP_ID"),
            "KAFKA_FLV_GROUP_ID": os.environ.get("KAFKA_FLV_GROUP_ID"),
            "TOS_ACCESS_KEY": os.environ.get("TOS_ACCESS_KEY"),
            "TOS_SECRET_KEY": os.environ.get("TOS_SECRET_KEY"),
            "TOS_ENDPOINT": os.environ.get("TOS_ENDPOINT"),
            "TOS_REGION": os.environ.get("TOS_REGION"),
            "TOS_BUCKET": os.environ.get("TOS_BUCKET"),
            "TOS_PUBLIC_HOST": os.environ.get("TOS_PUBLIC_HOST"),
            "DATA_DIR": os.environ.get("DATA_DIR"),
            "LOG_LEVEL": os.environ.get("LOG_LEVEL"),
            "USE_PRESIGNED_URL": os.environ.get("USE_PRESIGNED_URL"),
        }

    def tearDown(self):
        # 中文注释：恢复测试前的环境变量与 .env 文件，避免污染其他测试。
        for key, value in self.original_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if self.original_env_content is None:
            self.env_path.unlink(missing_ok=True)
        else:
            self.env_path.write_text(self.original_env_content, encoding="utf-8")

    def test_base_config_reads_values_from_environment(self):
        # 中文注释：配置对象应直接读取环境变量，而不是依赖代码里的硬编码地址。
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka-a:9092,kafka-b:9092"
        os.environ["KAFKA_INPUT_TOPIC"] = "input-topic"
        os.environ["KAFKA_FLV_TOPIC"] = "flv-topic"
        os.environ["KAFKA_OUTPUT_TOPIC"] = "output-topic"
        os.environ["KAFKA_GROUP_ID"] = "group-a"
        os.environ["KAFKA_FLV_GROUP_ID"] = "group-b"
        os.environ["TOS_ACCESS_KEY"] = "demo-access-key"
        os.environ["TOS_SECRET_KEY"] = "demo-secret-key"
        os.environ["TOS_ENDPOINT"] = "https://tos.example.com"
        os.environ["TOS_REGION"] = "cn-test"
        os.environ["TOS_BUCKET"] = "bucket-a"
        os.environ["TOS_PUBLIC_HOST"] = "https://cdn.example.com"
        os.environ["DATA_DIR"] = "runtime-data"
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["USE_PRESIGNED_URL"] = "true"

        cfg = BaseConfig()

        self.assertEqual(cfg.kafka_bootstrap_servers, ["kafka-a:9092", "kafka-b:9092"])
        self.assertEqual(cfg.kafka_input_topic, "input-topic")
        self.assertEqual(cfg.kafka_flv_topic, "flv-topic")
        self.assertEqual(cfg.kafka_output_topic, "output-topic")
        self.assertEqual(cfg.kafka_group_id, "group-a")
        self.assertEqual(cfg.kafka_flv_group_id, "group-b")
        self.assertEqual(cfg.tos_access_key, "demo-access-key")
        self.assertEqual(cfg.tos_secret_key, "demo-secret-key")
        self.assertEqual(cfg.tos_endpoint, "https://tos.example.com")
        self.assertEqual(cfg.tos_region, "cn-test")
        self.assertEqual(cfg.tos_bucket, "bucket-a")
        self.assertEqual(cfg.tos_public_host, "https://cdn.example.com")
        self.assertEqual(cfg.data_dir, "runtime-data")
        self.assertEqual(cfg.log_level, "DEBUG")
        self.assertTrue(cfg.use_presigned_url)

    def test_get_config_loads_values_from_dotenv_file(self):
        # 中文注释：项目启动时应自动读取根目录 .env，让所有配置都集中在一个文件里。
        for key in self.original_values:
            os.environ.pop(key, None)

        self.env_path.write_text(
            "\n".join(
                [
                    "KAFKA_BOOTSTRAP_SERVERS=dotenv-kafka-1:9092,dotenv-kafka-2:9092",
                    "KAFKA_INPUT_TOPIC=dotenv-input",
                    "KAFKA_FLV_TOPIC=dotenv-flv",
                    "KAFKA_OUTPUT_TOPIC=dotenv-output",
                    "KAFKA_GROUP_ID=dotenv-group",
                    "KAFKA_FLV_GROUP_ID=dotenv-flv-group",
                    "TOS_ACCESS_KEY=dotenv-access",
                    "TOS_SECRET_KEY=dotenv-secret",
                    "TOS_ENDPOINT=https://dotenv-tos.example.com",
                    "TOS_REGION=cn-dotenv",
                    "TOS_BUCKET=dotenv-bucket",
                    "TOS_PUBLIC_HOST=https://dotenv-cdn.example.com",
                    "DATA_DIR=dotenv-data",
                    "LOG_LEVEL=WARNING",
                    "USE_PRESIGNED_URL=1",
                ]
            ),
            encoding="utf-8",
        )

        cfg = get_config()

        self.assertEqual(cfg.kafka_bootstrap_servers, ["dotenv-kafka-1:9092", "dotenv-kafka-2:9092"])
        self.assertEqual(cfg.kafka_input_topic, "dotenv-input")
        self.assertEqual(cfg.tos_access_key, "dotenv-access")
        self.assertEqual(cfg.tos_bucket, "dotenv-bucket")
        self.assertTrue(cfg.use_presigned_url)
