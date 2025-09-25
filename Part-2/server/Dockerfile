FROM python:3.10-slim

WORKDIR /app

# Install Postgres server + client
RUN apt-get update && apt-get install -y \
    postgresql postgresql-contrib \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY deploy.sh .

# Ensure script is Unix formatted and executable
RUN sed -i 's/\r$//' deploy.sh && chmod +x deploy.sh

EXPOSE 5000 5432

CMD ["./deploy.sh"]
