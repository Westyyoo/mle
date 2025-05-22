
FROM openjdk:11-jdk-slim


RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt jupyterlab


COPY . .


ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH


EXPOSE 8888


CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]