FROM alpine:latest

COPY requirements.txt mysql_to_csv.py ./

COPY crontab /etc/cron.d/pycron

RUN apk update && apk add dcron python2 py2-pip && rm -rf /var/cache/apk/*

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

VOLUME /app

#RUN mkdir -p /var/log/cron && mkdir -m 0644 -p /var/spool/cron/crontabs && touch /var/log/cron/cron.log && mkdir -m 0644 -p /etc/cron.d

CMD ["cron", "-f"]
