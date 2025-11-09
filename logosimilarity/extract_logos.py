from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
import requests
import os
import datetime

LOG_FILE = "logo_fetch_log.txt"

def log_message(msg: str):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

def fetch_logo(domain: str):
    if not domain:
        log_message(f"Empty domain value.")
        return None

    sources = [
        f"https://www.google.com/s2/favicons?sz=128&domain={domain}",
        f"https://logo.clearbit.com/{domain}",
        f"https://icons.duckduckgo.com/ip3/{domain}.ico",
        f"https://{domain}/favicon.ico",
    ]

    for url in sources:
        try:
            resp = requests.get(url, timeout=1, allow_redirects=True)
            str_status = str(resp.status_code)
            if  (str_status.startswith("2") or str_status.startswith("3")) and \
               resp.headers.get("Content-Type", "").startswith("image"):
                log_message(f"Found logo for {domain}: {url}")
                return resp.content
            else:
                log_message(f"Not an image or bad status for {domain}: {url} ({resp.status_code})")
        except Exception as e:
            log_message(f"Error fetching {domain} from {url}: {e}")
            continue

    log_message(f"No valid logo found for {domain}")
    return None

fetch_logo_udf = udf(fetch_logo, BinaryType())

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LogoFetcher") \
        .master("local[*]") \
        .getOrCreate()

    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    df = spark.read.option("header", "true").parquet("logos.snappy.parquet")

    df.show(5, truncate=False)

    df_with_logos = df.withColumn("logo", fetch_logo_udf(df["domain"]))

    df_with_logos.write.mode("overwrite").parquet("domain_logos_binary")

    df_with_logos.show(truncate=False)

    spark.stop()
