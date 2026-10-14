# Start with Python 3.11 slim (small size!)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first (Docker cache trick!)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all code
COPY . .

# Security: don't run as root!
RUN adduser --disabled-password appuser
USER appuser

# Run pipeline
CMD ["python", "src/pipeline.py"]