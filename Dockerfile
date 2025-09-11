FROM python:3.12-slim

# Python environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Set your custom environment variable
ENV YOUR_ENV=wellness_aggregator

# Install dependencies
RUN apt-get update && apt-get install -y curl git && rm -rf /var/lib/apt/lists/*

# Set the working directory for the application
WORKDIR /app

# Ensure README.md exists (fixes the metadata error)
COPY README.md /app/
RUN test -f README.md || echo "# UV App" > README.md

# Copy only the necessary files first (for better caching)
COPY pyproject.toml /app/
COPY requirements.txt /app/

# Install dependencies using pip (forcing PEP 517 recognition)
RUN pip install --no-cache-dir --use-pep517 .

# Copy the rest of the application
COPY . /app/

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Set the working directory to ensure `runner.py` runs in the right path
WORKDIR /app

# Run the application
CMD ["python", "uv_app/run_all.py"]