# 基础镜像
FROM scxhtb-registry.cn-hangzhou.cr.aliyuncs.com/xhtb/java:8

# 维护者信息
LABEL authors="chuanyun"

# 定义时区参数
ENV TZ=Asia/Shanghai
# 限制 glibc 分配区数量，减轻「JVM 回收正常但 Docker RSS 持续增长」（堆外/原生内存不归还 OS）
ENV MALLOC_ARENA_MAX=2

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 传递版本号参数
ARG DBSYNCER_VERSION
# 设置环境变量，确保运行时能读取
ENV DBSYNCER_VERSION=${DBSYNCER_VERSION}

# 设置工作目录
WORKDIR /app

# 复制压缩包
COPY target/dbsyncer-$DBSYNCER_VERSION-bin.zip .

# 解压、清理并重命名目录
RUN unzip dbsyncer-$DBSYNCER_VERSION-bin.zip && \
    rm -rf dbsyncer-$DBSYNCER_VERSION-bin.zip && \
    mv dbsyncer-$DBSYNCER_VERSION dbsyncer && \
    # 进入应用目录进行初始化操作
    cd dbsyncer && \
    # 添加执行权限
    chmod +x bin/startup-docker.sh && \
    # 创建必要的目录 (虽然 VOLUME 会挂载，但为了权限和结构完整，建议创建)
    mkdir -p logs conf data plugins

# 暴露端口
EXPOSE 18686

# 挂载卷 (注意：如果宿主机挂载了这些目录，容器内的原有文件会被隐藏)
VOLUME [ "/app/dbsyncer/conf", "/app/dbsyncer/data", "/app/dbsyncer/logs", "/app/dbsyncer/plugins" ]

# 设置工作目录为应用根目录
WORKDIR /app/dbsyncer

# 启动命令
# 使用 exec 形式执行脚本，确保脚本是 PID 1
ENTRYPOINT ["/bin/bash", "-c", "/app/dbsyncer/bin/startup-docker.sh"]