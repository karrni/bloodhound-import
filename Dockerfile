# Node 16.20.1 on Alpine 3.18
FROM node:16.20.1-alpine3.18

WORKDIR /app

COPY . .

RUN npm install --omit=dev && \
    chmod +x docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
