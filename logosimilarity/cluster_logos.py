from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_set, least
from pyspark.sql.types import IntegerType, StringType


SIM_THRESHOLD = 5


def hamming_distance(h1, h2):
    if not h1 or not h2:
        return None
    try:
        return bin(int(h1, 16) ^ int(h2, 16)).count("1")
    except Exception:
        return None


hamming_udf = udf(hamming_distance, IntegerType())


@udf(StringType())
def min_from_array(arr):
    if arr and len(arr) > 0:
        return min(arr)
    return None


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("LogoClustering")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet("logos_with_phash")

        df_a = df.select(col("domain").alias("domain_a"), col("phash").alias("phash_a"))
        df_b = df.select(col("domain").alias("domain_b"), col("phash").alias("phash_b"))

        pairs = df_a.crossJoin(df_b).filter(col("domain_a") < col("domain_b"))
        pairs = pairs.withColumn("dist", hamming_udf(col("phash_a"), col("phash_b")))

        edges = pairs.filter(col("dist") <= SIM_THRESHOLD) \
                     .select(col("domain_a").alias("src"), col("domain_b").alias("dst"))

        print(f"Found {edges.count()} similar edges")

        vertices = df.select(col("domain").alias("id")) \
                     .withColumn("cluster", col("id"))
        changed = True
        iteration = 0
        MAX_ITER = 10

        prev_vertices = None

        while changed and iteration < MAX_ITER:
            iteration += 1
            print(f"\nIteration {iteration}")

            edges_rev = edges.select(col("dst").alias("src"), col("src").alias("dst"))
            all_edges = edges.union(edges_rev)

            joined = (
                all_edges.join(vertices, all_edges.src == vertices.id, "left")
                .select(col("dst").alias("id"), col("cluster"))
            )

            new_clusters = (
                joined.groupBy("id")
                .agg(min_from_array(collect_set("cluster")).alias("new_cluster"))
            )

            prev_vertices = vertices.alias("prev")

            vertices = (
                vertices.alias("curr")
                .join(new_clusters.alias("new"), col("curr.id") == col("new.id"), "left")
                .select(
                    col("curr.id").alias("id"),
                    least(col("curr.cluster"), col("new.new_cluster")).alias("cluster")
                )
            )

            v_curr = vertices.alias("curr")
            v_prev = prev_vertices.alias("prev")

            diff = (
                v_curr.join(v_prev, v_curr.id == v_prev.id)
                .filter(col("curr.cluster") != col("prev.cluster"))
                .count()
            )

            changed = diff > 0
            print(f"changed nodes: {diff}")

        vertices = vertices.withColumnRenamed("cluster", "cluster_id")
        vertices.write.mode("overwrite").parquet("logo_clusters")

        print(f"\nConverged after {iteration} iterations.")

    except Exception as e:
        print("ERROR:", e)

    finally:
        spark.stop()
