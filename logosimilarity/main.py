from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, size, col


def show_clusters(spark, path="logo_clusters", min_size=1):

    df_clusters = spark.read.parquet(path)

    grouped = (
        df_clusters.groupBy("cluster_id")
        .agg(collect_list("id").alias("domains"))
        .withColumn("size", size(col("domains")))
        .filter(col("size") >= min_size)
        .orderBy(col("size").desc())
    )

    total_clusters = grouped.count()
    print(f"Total clustere (cu dimensiune mai mare de {min_size}): {total_clusters}\n")

    clusters = grouped.collect()
    for i, row in enumerate(clusters, 1):
        print("=" * 80)
        print(f"Cluster {i}: {row['cluster_id']}  (size={row['size']})")
        print("-" * 80)
        for domain in row["domains"]:
            print(f"   • {domain}")
        print()


    grouped.select("cluster_id", "size").write.mode("overwrite").csv("clusters_summary_csv")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("ShowAllLogoClusters")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        show_clusters(spark, path="logo_clusters", min_size=2)

    except Exception as e:
        print("ERROR:", e)

    finally:
        spark.stop()

