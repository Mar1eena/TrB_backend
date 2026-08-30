FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY internal/services/indicators/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Слой сбрасывается, когда на PyPI выходит новая версия trb-proto.
ADD https://pypi.org/pypi/trb-proto/json /tmp/trb-proto.json
RUN pip install --no-cache-dir --upgrade trb-proto && rm -f /tmp/trb-proto.json

COPY internal/services/indicators/*.py /app/

EXPOSE 9093

CMD ["python", "/app/main.py"]
