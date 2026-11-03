# Python image for the Kafka replay producer, the Streamlit dashboard and the tests.
FROM python:3.12-slim

ARG UID=1000
ARG GID=1000

COPY docker/app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN groupadd -g ${GID} app && useradd -m -u ${UID} -g ${GID} app
USER app
WORKDIR /app
ENV PYTHONUNBUFFERED=1
