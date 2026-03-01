FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python faker
COPY generate.py .
CMD ["python", "generate.py"]
