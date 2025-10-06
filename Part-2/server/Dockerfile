# Stage 1: Python environment
FROM python:3.10-slim AS builder
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: final runtime
FROM python:3.10-slim
WORKDIR /app

# Install Postgres server and client
RUN apt-get update && apt-get install -y \
    postgresql postgresql-contrib libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

# Copy app files
COPY app.py .
COPY deploy.sh .

# Make deploy.sh executable
RUN chmod +x deploy.sh

# Expose ports
EXPOSE 5000 5432

# Start the app
CMD ["./deploy.sh"]
