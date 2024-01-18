FROM docker.io/java:8
LABEL authors="life"
#定义时区参数
ENV TZ=Asia/Shanghai
#设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo '$TZ' > /etc/timezone


WORKDIR /app

COPY /target/dbsyncer-2.0.1-Beta-bin.zip /app

RUN unzip dbsyncer-2.0.1-Beta-bin.zip

RUN cd dbsyncer-2.0.1-Beta && \
    cd bin && \
    chmod +x startup.sh \
    ./startup.sh

EXPOSE 18686

ENTRYPOINT ["/bin/bash","-c","/app/dbsyncer-2.0.1-Beta/bin/startup-docker.sh"]