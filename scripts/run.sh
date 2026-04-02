#!/usr/bin/env bash
set -e
export APP_ENV=${APP_ENV:-dev}
uvicorn app.main:app --host 0.0.0.0 --port 8000
