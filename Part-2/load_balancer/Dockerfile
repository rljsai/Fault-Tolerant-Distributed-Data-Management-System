# Stage 1: Build Python environment
FROM python:3.10-slim AS builder
WORKDIR /app
COPY requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Final runtime
FROM python:3.10-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY . .

# Install Docker CLI for aiodocker
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

EXPOSE 8000
CMD ["python", "lb.py"]
