import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import spark.hdfs_stream_archiver as hsa
import pandas as pd
import json

# Test 1: Deserializer
valid = hsa.safe_json_deserializer(json.dumps({"event_id": "1", "rating": 5}).encode('utf-8'))
assert valid is not None and valid['rating'] == 5, 'Deserializer valid failed'
invalid = hsa.safe_json_deserializer(b'not json')
assert invalid is None, 'Deserializer invalid failed'

# Test 2: upload with empty df
assert hsa.upload_parquet_to_hdfs(pd.DataFrame(), 'dummy_path') is True

# Test 3: check hdfs alive function
is_alive = hsa.is_hdfs_alive()
print(f'HDFS alive: {is_alive}')
print('ALL ARCHIVER TESTS PASSED!')
