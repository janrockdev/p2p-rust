import pyarrow as pa
import pyarrow.ipc as ipc

# Arrow IPC
with ipc.open_file("node_8080_cache.arrow") as reader:
    print("Schema:\n",reader.schema)
    for i in range(reader.num_record_batches):
        batch = reader.get_batch(i)
        print(batch.to_pandas())
