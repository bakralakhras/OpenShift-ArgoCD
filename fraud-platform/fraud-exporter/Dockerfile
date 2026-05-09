FROM python:3.11-slim
RUN pip install --no-cache-dir requests
WORKDIR /app
COPY exporter.py .
EXPOSE 9090
CMD ["python3", "exporter.py"]
