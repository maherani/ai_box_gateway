FROM python:3.11-alpine

WORKDIR /app
COPY proxy.py /app/proxy.py
COPY gid_mapper.py /app/gid_mapper.py

ENV PYTHONUNBUFFERED=1
ENV BACKEND_HOST=anythingllm
ENV BACKEND_PORT=3001
ENV GATEWAY_PORT=3001
ENV MAX_FILE_SIZE_BYTES=2097152

EXPOSE 3001

CMD ["python", "/app/proxy.py"]