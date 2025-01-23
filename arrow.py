import pyarrow as pa
import pyarrow.ipc as ipc

# Open the Arrow IPC file
with ipc.open_file("node_8080_cache.arrow") as reader:
    print("Schema:", reader.schema)  # Display the schema

    # Iterate over the record batches
    for i in range(reader.num_record_batches):
        batch = reader.get_batch(i)
        print(batch.to_pandas())  # Convert to a Pandas DataFrame and print
