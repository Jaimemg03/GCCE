FROM postgres:16-alpine

COPY init-scripts/ /docker-entrypoint-initdb.d/

RUN chmod -R +r /docker-entrypoint-initdb.d/ && chmod +x /docker-entrypoint-initdb.d/*.sh
