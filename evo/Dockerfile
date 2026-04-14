FROM node:24-alpine AS builder

RUN apk update && \
    apk add --no-cache git ffmpeg wget curl bash openssl

LABEL version="2.3.1" description="Api to control whatsapp features through http requests." 
LABEL maintainer="Davidson Gomes" git="https://github.com/DavidsonGomes"
LABEL contact="contato@evo-api.com"

WORKDIR /evo

COPY ./package*.json ./
COPY ./tsconfig.json ./
COPY ./tsup.config.ts ./

RUN npm ci --silent

COPY ./src ./src
COPY ./public ./public
COPY ./prisma ./prisma
COPY ./manager ./manager
COPY ./.env.example ./.env
COPY ./runWithProvider.js ./

COPY ./Docker ./Docker

RUN chmod +x ./Docker/scripts/* && dos2unix ./Docker/scripts/*

RUN ./Docker/scripts/generate_database.sh

RUN npm run build

FROM node:24-alpine AS final

RUN apk update && \
    apk add tzdata ffmpeg bash openssl

ENV TZ=America/Sao_Paulo
ENV DOCKER_ENV=true

WORKDIR /evo

COPY --from=builder /evo/package.json ./package.json
COPY --from=builder /evo/package-lock.json ./package-lock.json

COPY --from=builder /evo/node_modules ./node_modules
COPY --from=builder /evo/dist ./dist
COPY --from=builder /evo/prisma ./prisma
COPY --from=builder /evo/manager ./manager
COPY --from=builder /evo/public ./public
COPY --from=builder /evo/.env ./.env
COPY --from=builder /evo/Docker ./Docker
COPY --from=builder /evo/runWithProvider.js ./runWithProvider.js
COPY --from=builder /evo/tsup.config.ts ./tsup.config.ts

ENV DOCKER_ENV=true

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "-c", ". ./Docker/scripts/deploy_database.sh && npm run start:prod" ]
