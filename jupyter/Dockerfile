FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.1.0 \
    scikit-learn \
    matplotlib \
    seaborn \
    plotly \
    boto3 \
    trino

# Download Delta JARs into Spark's jars folder
ENV SPARK_HOME=/usr/local/spark
RUN curl -fsSL -o $SPARK_HOME/jars/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -fsSL -o $SPARK_HOME/jars/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -fsSL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER $NB_UID
