# Multi-architecture base image for AWS ECS Fargate & Cloud Tasks
FROM python:3.12-slim

# Prevent Python from writing .pyc files and enable unbuffered output for AWS CloudWatch
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TEMP_DIR=/tmp

WORKDIR /app

# Install system certificates and required network utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir python-dotenv

# Copy application source code
COPY ausem.py .

# Default execution
ENTRYPOINT ["python", "-u"]
CMD ["ausem.py"]
