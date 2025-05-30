FROM quay.io/astronomer/astro-runtime:9.2.0

USER root

# Install Java and Spark
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    libgeos-dev \
    curl \
    unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Spark
ENV SPARK_VERSION=3.5.0
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Switch back to astro user
USER astro

# Install requirements
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

