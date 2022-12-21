FROM python:3.8-slim-buster

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

RUN chmod a+x run.sh
ENTRYPOINT ["./run.sh"]
