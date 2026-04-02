# 视频分块上传服务

## 概述
- 使用 `FastAPI` 提供健康检查与运行入口，进程由 `uvicorn` 启动。
- 消费 Kafka 输入主题中的视频地址，下载到本地并按 30MiB 进行分块。
- 将分块上传至字节火山 TOS，发送包含上传结果的消息到 Kafka 输出主题。

## 依赖
- Python 3.10+
- `fastapi`、`uvicorn[standard]`、`kafka-python`、`requests`、`tos`、`pydantic`、`tenacity`、`python-dotenv`
- Kafka（本地可通过 `docker-compose` 启动）

## 环境变量
- `KAFKA_BOOTSTRAP_SERVERS`（默认 `localhost:9092`）
- `KAFKA_INPUT_TOPIC`（默认 `video.url.input`）
- `KAFKA_OUTPUT_TOPIC`（默认 `video.chunks.output`）
- `KAFKA_GROUP_ID`（默认 `video-chunker`）
- `TOS_ACCESS_KEY`、`TOS_SECRET_KEY`、`TOS_ENDPOINT`、`TOS_REGION`、`TOS_BUCKET`
- `DATA_DIR`（默认 `data`）
- `LOG_LEVEL`（默认 `INFO`）
- `TOS_PUBLIC_URL_TEMPLATE`（可选，公共 URL 模板，示例 `https://tos-cn-beijing.volces.com`）

## 本地开发
- 安装依赖：`pip install -r requirements.txt`
- 启动本地 Kafka：`docker-compose -f scripts/docker-compose.kafka.yml up -d`
- 启动服务：`./scripts/run.sh`
- 健康检查：`GET http://localhost:8000/healthz`

## 生产运行（PM2）
- 确保已安装 `pm2`
- 启动：`pm2 start ecosystem-pre.config.js && pm2 save && pm2 startup`
- 状态与日志：`pm2 status`、`pm2 logs video-chunker-service`
- 启动命令采用：`uvicorn app.main:app --host 0.0.0.0 --port 8000`

## 消息格式
- 输入：`{"source_url":"https://example.com/video.mp4","prefix":"projects/prod"}`
- 输出：包含 `job_id`、`source_url`、`total_size`、`chunk_size`、`chunks[{index,size,object_key,url}]`

## 注意事项
- TOS 上传需要具备 `tos:PutObject` 权限；对象命名避免字典序递增热点。
- 私有桶时返回 `object_key`；如需公共访问可配置公共策略或使用预签名 URL。

## 目录结构
- `app/main.py`：应用入口与生命周期
- `app/config/`：配置模块（`base.py`、`dev.py`、`prod.py`）
- `app/services/kafka/`：生产与消费封装
- `app/services/tos/`：TOS 客户端封装
- `app/services/video/`：下载与分块
- `app/models/`：Pydantic 模型
- `app/utils/`：日志与重试工具
- `scripts/`：运行与 Kafka 启动脚本

## 部署建议
- 使用容器化部署，配置 `APP_ENV=prod`，通过环境变量注入密钥与连接参数。
- 启用 `pm2-logrotate` 做日志轮转。
