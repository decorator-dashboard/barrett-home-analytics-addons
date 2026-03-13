ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app

RUN apk add --no-cache \
  python3 \
  py3-pip \
  ca-certificates

COPY app/requirements.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /app/requirements.txt

COPY app /app
COPY run.sh /run.sh

RUN chmod 755 /run.sh

CMD ["/run.sh"]
