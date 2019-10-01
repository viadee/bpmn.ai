FROM maven:3.6-jdk-8-alpine AS build-env

COPY / /home
WORKDIR /home
RUN mvn clean package -DskipTests



FROM ubuntu:18.04

LABEL maintainer="mario.micudaj@viadee.de"

RUN apt-get update && \
    apt-get -y install openjdk-8-jre-headless && \
    apt-get -y install libc6 && \
    apt-get clean

RUN addgroup --gid 1000 appuser && \
    adduser --uid 1000 --gid 1000 appuser && \
    mkdir -p /app && \
    mkdir -p /data

ARG APP_COMPONENT_DIR=/home/target

COPY --from=build-env ${APP_COMPONENT_DIR}/lib /app/lib
COPY --from=build-env ${APP_COMPONENT_DIR}/dependency /app/bin
COPY --from=build-env ${APP_COMPONENT_DIR}/dependency/META-INF /app/bin/META-INF

RUN chown -R appuser:appuser /app && \
    chown -R appuser:appuser /data && \
    find /app -type d -exec chmod 550 {} + && \
    find /app -type f -exec chmod 660 {} + && \
    chmod 770 /data

VOLUME /data
USER appuser

WORKDIR /data
ENTRYPOINT ["/usr/bin/java", "-Dspark.master=local[*]", "-cp", "/app/bin:/app/lib/*"]