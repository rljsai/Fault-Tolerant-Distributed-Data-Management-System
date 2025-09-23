FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# keep docker cli for debugging (optional)
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

CMD ["python", "lb.py"]
