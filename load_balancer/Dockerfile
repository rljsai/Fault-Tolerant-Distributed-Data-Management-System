FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# install docker cli inside container
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

CMD ["python", "lb.py"]
