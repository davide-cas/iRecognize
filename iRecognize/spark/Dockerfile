FROM jupyter/pyspark-notebook:spark-3.1.1

RUN pip3 install pyspark kafka-utils elasticsearch matplotlib requests Pillow

WORKDIR /app
ADD analyze.py .

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.1", "--master", "local[*]", "analyze.py"]