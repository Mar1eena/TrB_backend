# Используем Ubuntu 22.04: для focal (20.04) в репозитории 1c-17 пакеты отсутствуют
FROM ubuntu:22.04

# Устанавливаем переменные окружения для избежания вопросов при установке
ENV DEBIAN_FRONTEND=noninteractive

# Создаем пользователя и группу для PostgreSQL
RUN groupadd admin --gid=999 \
  && useradd --gid admin --uid=999 admin

# Устанавливаем gosu для корректного переключения пользователей
ENV SERVER_VERSION 17
ENV GOSU_VERSION 1.7

RUN apt-get -qq update \
  && apt-get -qq install --yes --no-install-recommends ca-certificates wget locales \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && wget --quiet -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
  && chmod +x /usr/local/bin/gosu \
  && gosu nobody true

# Обновляем пакеты и устанавливаем необходимые зависимости
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    apt-utils \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Генерируем локаль UTF-8
RUN locale-gen en_US.UTF-8 && update-locale LANG=en_US.UTF-8

# Устанавливаем русскую локаль
RUN localedef --inputfile ru_RU --force --charmap UTF-8 --alias-file /usr/share/locale/locale.alias ru_RU.UTF-8
ENV LANG ru_RU.utf8

# Скачиваем и запускаем скрипт для добавления репозитория PostgresPro 1C
# (std-17 требует access key; 1c-17 — открытый репозиторий для 1С)
RUN wget https://repo.postgrespro.ru/1c/1c-17/keys/pgpro-repo-add.sh \
    && sh pgpro-repo-add.sh

# Устанавливаем PostgreSQL для 1С
RUN apt-get update && apt-get install postgrespro-1c-17 -y \
  && rm -rf /var/lib/pgpro/1c-17/data \
  && mkdir -p /var/lib/pgpro/1c-17/data \
  && chown -R postgres:postgres /var/lib/pgpro/1c-17/data

ENV PGDATA /var/lib/pgpro/1c-17/data

# Создаем директории и настраиваем права
RUN mkdir --parent /var/run/postgresql "$PGDATA" /docker-entrypoint-initdb.d \
  && chown --recursive postgres:postgres /var/run/postgresql "$PGDATA" \
  && chmod g+s /var/run/postgresql

ENV PATH /opt/pgpro/1c-17/bin:$PATH

# Копируем скрипты для инициализации (sed убирает CRLF с Windows)
COPY docker-entrypoint.sh /
COPY postgresql.conf.sh /docker-entrypoint-initdb.d
RUN sed -i 's/\r$//' /docker-entrypoint.sh /docker-entrypoint-initdb.d/postgresql.conf.sh \
  && chmod +x /docker-entrypoint.sh /docker-entrypoint-initdb.d/postgresql.conf.sh

# Устанавливаем точку входа
ENTRYPOINT ["/bin/bash", "/docker-entrypoint.sh"]

# Определяем том для данных
VOLUME $PGDATA

# Открываем порт для PostgreSQL
EXPOSE 5432

# Запускаем PostgreSQL с настройками
CMD ["postgres"]