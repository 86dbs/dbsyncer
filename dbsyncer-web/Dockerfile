FROM java:8
LABEL authors="life"
#定义时区参数
ENV TZ=Asia/Shanghai
#设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo '$TZ' > /etc/timezone
ARG DBSYNCER_VERSION
# 设置环境变量，确保在运行时能被读取
ENV DBSYNCER_VERSION=${DBSYNCER_VERSION}

WORKDIR /app
COPY target/dbsyncer-$DBSYNCER_VERSION-bin.zip /app

RUN unzip dbsyncer-$DBSYNCER_VERSION-bin.zip && \
    rm -rf dbsyncer-$DBSYNCER_VERSION-bin.zip && \
    mv dbsyncer-$DBSYNCER_VERSION dbsyncer

RUN cd dbsyncer && \
    cd bin && \
    chmod +x startup.sh \
    ./startup.sh

EXPOSE 18686
VOLUME [ "/app/dbsyncer/conf", "/app/dbsyncer/data", "/app/dbsyncer/logs" ,"/app/dbsyncer/plugins"]
ENTRYPOINT ["/bin/bash","-c","/app/dbsyncer/bin/startup-docker.sh"]