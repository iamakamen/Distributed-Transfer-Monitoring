FROM python:3.12-slim

WORKDIR /app

# System deps (optional minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir \
    prometheus-client \
    pandas \
    scikit-learn

# Default command (can be overridden by docker-compose)
CMD ["python", "-m", "exporter.exporter"]
