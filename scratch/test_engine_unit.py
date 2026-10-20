import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from pyspark.sql import SparkSession
from spark.tourism_engine import SparkTourismCoreEngine

def main():
    print("[*] Khởi động unit test cho SparkTourismCoreEngine...")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("EngineUnitTest") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    csv_path = os.path.join(BASE_DIR, "data", "tourism_dataset_clean.csv")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(csv_path)

    engine = SparkTourismCoreEngine(spark=spark, df=df)

    assert len(engine.catalog) == 317, f"Catalog len {len(engine.catalog)} != 317"
    assert len(engine.centers) == 5, f"Centers count {len(engine.centers)} != 5"
    assert len(engine.association_rules) > 0, "No association rules generated"
    itin = engine.generate_itinerary('Khánh Hòa', 1)
    assert len(itin) == 1, "Itinerary days != 1"
    assert len(itin[0]['stops']) > 0, "Itinerary stops empty"

    spark.stop()
    print("[PASS] SparkTourismCoreEngine unit test passed 100%!")

if __name__ == '__main__':
    main()
