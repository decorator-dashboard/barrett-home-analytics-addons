ARG BUILD_FROM
FROM ${BUILD_FROM}

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-pip python3-venv ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY app/package.json /app/package.json
RUN npm install --omit=dev

COPY app/requirements.txt /app/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /app/requirements.txt

COPY app /app
COPY run.sh /run.sh

RUN chmod 755 /run.sh

CMD ["/run.sh"]

