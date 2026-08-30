FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY internal/services/indicators/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --from=proto pyproject.toml /opt/TrB_proto/pyproject.toml
COPY --from=proto gen/python /opt/TrB_proto/gen/python
RUN pip install --no-cache-dir /opt/TrB_proto && rm -rf /opt/TrB_proto

COPY internal/services/indicators/*.py /app/

EXPOSE 9093

CMD ["python", "/app/main.py"]
