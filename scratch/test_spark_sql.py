from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

spark = SparkSession.builder.master('local[1]').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create sample dataset
base_df = spark.sql("""
    SELECT 1 as stt, 'Bai Dai' as name, 'bien tam bien dep' as text_for_tfidf
    UNION ALL
    SELECT 2 as stt, 'Chua Linh Ung' as name, 'chua tam linh di tich' as text_for_tfidf
""")

tokenizer = Tokenizer(inputCol="text_for_tfidf", outputCol="words")
words_df = tokenizer.transform(base_df)
hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=2000)
tf_df = hashing_tf.transform(words_df)
idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(tf_df)
tfidf_df = idf_model.transform(tf_df)

print("Base vectorized successfully!")

# Now vectorize presets using SQL DataFrame (pure Spark, no cloudpickle RDD!)
preset_df = spark.sql("""
    SELECT 'bien' AS preset_id, 'bien tam bien' AS text_for_tfidf
    UNION ALL
    SELECT 'tam_linh' AS preset_id, 'chua tam linh' AS text_for_tfidf
""")

q_words = tokenizer.transform(preset_df)
q_tf = hashing_tf.transform(q_words)
q_tfidf = idf_model.transform(q_tf)

rows = q_tfidf.select('preset_id', 'features').collect()
for r in rows:
    print("Preset:", r['preset_id'], "SparseVector indices:", r['features'].indices, "values:", r['features'].values)
