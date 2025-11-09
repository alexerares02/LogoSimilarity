from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from PIL import Image
import imagehash
import io

def compute_phash(logo_bytes):
    try:
        if logo_bytes is None:
            return None
        image = Image.open(io.BytesIO(logo_bytes)).convert("RGB")
        hash_val = imagehash.phash(image)
        return str(hash_val)
    except Exception:
        return None

phash_udf = udf(compute_phash, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ComputeLogoPHash") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.parquet("domain_logos_binary")
    df_2 = spark.read.parquet("logos.snappy.parquet")

    df_with_phash = df.withColumn("phash", phash_udf(col("logo")))

    df_with_phash.filter(col("phash").isNotNull()) \
                 .write.mode("overwrite") \
                 .parquet("logos_with_phash")

    spark.stop()
