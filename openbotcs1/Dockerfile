# 基础镜像（slim 减少体积；bookworm=Debian 12，仍在支持期。bullseye 已 EOL，security 源失效）
FROM python:3.11-slim-bookworm

# 维护者信息
LABEL maintainer="OpenBot Maintainer"
LABEL version="2.5"
LABEL description="OpenBot - Telegram 插件化下载机器人（Bot API + MTProto + Web查看器）"

# 设置时区（解决日志/定时任务时区问题）
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 安装系统依赖（ffmpeg + 基础工具）
# 网络说明：Docker Desktop 已配置代理（Clash 127.0.0.1:7897），apt 直接走代理用官方源最稳定
RUN apt-get update -o Acquire::Retries=5 \
    && for i in 1 2 3 4 5; do apt-get install -y --no-install-recommends -o Acquire::Retries=5 ffmpeg curl git && break || sleep 5; done \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 创建非root用户（提升安全性）
RUN groupadd -r openbot && useradd -r -g openbot openbot

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装（先复制requirements，利用Docker缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip cache purge

# 复制项目代码
COPY . .

# 创建运行时目录并授权（data 用于持久化 look.db 等数据；页面随插件打包，无独立 web 目录）
RUN mkdir -p /app/logs /app/sessions /app/download /app/data \
    && chown -R openbot:openbot /app

# 切换到非root用户
USER openbot

# Web 查看器端口（bot 本身无需端口）
EXPOSE 7777

# 启动命令：先后台启动 Web 查看器（7777），再启动主 bot 进程
CMD ["sh", "-c", "python features/look/look_server.py & python main.py"]
