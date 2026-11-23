FROM python:3.12-slim
WORKDIR /opt/pipeline
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-m", "pipeline.run", "all", "--days", "30"]
