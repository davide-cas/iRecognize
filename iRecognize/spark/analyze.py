from numpy import result_type
from pyspark.sql.functions import udf
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
import time

##########################################
##########################################
##########################################

# making the post request to get the last prediction
# plotting the last tag, bbox and percentage prediction
# sending back the modified image to the telegram user
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from PIL import Image
import requests
import json
import random


@udf
def get_user(file_name):
    user_name = file_name.split("_")[0]
    user_id = file_name.split("_")[1]

    user = user_name + " (" + user_id + ")"
    return user


import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from PIL import Image
import requests, json, random

def get_prediction(file_name):
    training_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    iteration_id = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
    project_id = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"

    # Load Image
    img = Image.open("/usr/share/logstash/csv/photos/" + file_name)
    width, height = img.size

    # Azure Custom Vision API Connection
    url = f"https://southcentralus.api.cognitive.microsoft.com/customvision/v3.3/Training/projects/{project_id}/quicktest/image"

    headers = {
        # Request headers
        "Content-Type": "multipart/form-data",
        "Training-key": training_key,
    }

    params = {
        # Request parameters
        "iterationId": iteration_id,
        "store": "True",
    }

    resp = requests.post(url, headers=headers, params=params, data=open("/usr/share/logstash/csv/photos/" + file_name, "rb").read())
    r = resp.json()
    results = r["predictions"]
    n_predictions = len(results)

    tags_detected = []

    for i in range(n_predictions):
        probability = results[i]["probability"]

        if probability > 0.60:
            tagName = results[i]["tagName"]
            tags_detected.append(tagName)

            percentage = str(round((results[i]["probability"] * 100), 2)) + "%"

            x = results[i]["boundingBox"]["left"] * width
            y = results[i]["boundingBox"]["top"] * height
            w = results[i]["boundingBox"]["width"] * width
            h = results[i]["boundingBox"]["height"] * height

            # Display the image
            obj = plt.imshow(img)

            color = "#%06x" % random.randint(0, 0xFFFFFF)
            text = tagName + ": " + percentage

            # Add the patch to the Axes
            plt.gca().add_patch(Rectangle((x, y), w, h, linewidth=2, edgecolor=color, facecolor="none"))
            plt.text(x + w/2, y, text, fontsize=12.5/width*height, fontweight="bold", ha="center", va="bottom", fontfamily="monospace", color=color, bbox={"facecolor": "white", "alpha": 0.75, "pad": 2.5})

        else: break


    if len(tags_detected) == 0:
        tags_detected = "NO DETECTION"
        plt.savefig(f"/usr/share/logstash/csv/detected_photos/EMPTY_{file_name}", dpi=400)
        plt.figure()

    else:
        # Getting current axes
        a = plt.gca()
        a.axis("off")

        plt.savefig(f"/usr/share/logstash/csv/detected_photos/{file_name}", dpi=400)
        plt.figure()
        
    result = {"tags_detected" : tags_detected}

    return result

##########################################
##########################################
##########################################


kafkaServer = "kafkaserver:9092"
elastic_host = "http://elasticsearch"

elastic_topic = "tap"
elastic_index = "tap"

'''
es_mapping = {
    "mappings": {
        "properties": {
            "file_name": {"type": "text"},
            # "tags_detected": {"type": "text"}
        }
    }
}
'''

es = Elasticsearch(hosts=elastic_host)
while not es.ping(): time.sleep(1)

es.indices.create(
    index = elastic_index,
    #body = es_mapping,
    ignore = 400  # ignore 400 already exists code
)

# Spark Configuration
sparkConf = SparkConf().set("spark.app.name", "network-tap") \
    .set("es.nodes", elastic_host) \
    .set("es.port", "9200") \
    .set("spark.executor.heartbeatInterval", "200000") \
    .set("spark.network.timeout", "300000")

sc = SparkContext.getOrCreate(conf=sparkConf)
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("WARN")

input_struct = tp.StructType([
    tp.StructField(name="file_name", dataType=tp.StringType(), nullable=True)
])

data_struct = tp.StructType([
    tp.StructField(name="tags_detected", dataType=tp.ArrayType(tp.StringType()), nullable=True)
])

# Applying the "get_prediction" defined function
get_prediction_udf = udf(get_prediction, data_struct)

# Reading Kafka stream
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", elastic_topic) \
    .option("startingOffset", "earliest") \
    .load()

# Data Select
df_kafka = df_kafka.selectExpr("CAST(value AS STRING)")\
    .select(from_json("value", input_struct).alias("data"))\
    .select("data.*")

# Adding User ID
df_kafka = df_kafka.withColumn("user_id", get_user(df_kafka.file_name))

# Adding Tags
df_kafka = df_kafka.withColumn("tags_detected", get_prediction_udf(df_kafka.file_name))


df_kafka = df_kafka.select("user_id", "tags_detected")

# Outputting list of classes to Elastic Search
df_kafka \
    .writeStream \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .format("es") \
    .start(elastic_index) \
    .awaitTermination()
