FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update \
    && apt upgrade -y --fix-missing --no-install-recommends libicu-dev \
    && apt-get clean -y && apt-get autoremove -y && apt-get autoclean -y && rm -r /var/lib/apt/lists/

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY main.py ./

CMD ["python", "main.py", "--config", "config.json", "--verbose"]
