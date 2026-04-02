module.exports = {
    apps: [
        {
            name: "video-cutter-api",
            cwd: ".",
            script: "python",
            args: "-m uvicorn app.main:app --host 0.0.0.0 --port 8000",
            interpreter: "none",
            exec_mode: "fork",
            autorestart: true,
            max_memory_restart: "512M",
            env: {
                APP_ENV: "prod",
                ENABLE_CONSUMER: "false",
                ENABLE_FLV_CONSUMER: "false",
                PYTHONPATH: "."
            }
        },
        {
            name: "video-cutter-consumer",
            cwd: ".",
            script: "python",
            args: "./scripts/run_consumer.py",
            interpreter: "none",
            exec_mode: "fork",
            instances: 3,
            process_name: "video-cutter-consumer-%i",
            autorestart: true,
            max_memory_restart: "512M",
            env: {
                APP_ENV: "prod",
                PYTHONPATH: "."
            }
        }
    ]
}
