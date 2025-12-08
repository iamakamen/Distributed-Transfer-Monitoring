FROM python:3.12-slim

# Install Java runtime for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default command (overridden by docker-compose per service)
CMD ["python", "-m", "exporter.exporter"]