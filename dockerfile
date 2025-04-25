# Dockerfile
FROM python:3.10-slim

# 创建应用目录并切进去
WORKDIR /app

# 先拷贝依赖清单并安装，利用缓存加速重建
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 再把项目所有代码拷到镜像里
COPY . .