//! Test: DataFusion reading a Parquet file from RawObjectStore.

mod common;


use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::prelude::*;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn datafusion_query_parquet_on_raw_store() {
    // 1. Create a RawObjectStore backed by a 64 MB temp file.
    let (store, _tmp) = common::make_store();
    let store = Arc::new(store);

    // 2. Build a small Parquet file in memory.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec!["alice", "bob", "charlie", "dave", "eve"]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();

    let mut buf = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // 3. PUT it into the store.
    let parquet_path = Path::from("data.parquet");
    store
        .put(&parquet_path, Bytes::from(buf).into())
        .await
        .unwrap();

    // 4. Register the store with DataFusion under the "raw://" scheme.
    let ctx = SessionContext::new();
    let url = Url::parse("raw://localhost").unwrap();
    ctx.register_object_store(&url, store.clone());

    // 5. Register the parquet table and query it.
    let table_url = "raw://localhost/data.parquet";
    ctx.register_parquet("my_table", table_url, ParquetReadOptions::default())
        .await
        .unwrap();

    let df = ctx
        .sql("SELECT id, name FROM my_table WHERE id > 2 ORDER BY id")
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // 6. Verify results.
    assert_eq!(results.len(), 1, "expected one batch");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[3, 4, 5]);

    // DataFusion may return StringArray or StringViewArray depending on version.
    let col = batch.column(1);
    let name_strings: Vec<String> = (0..col.len())
        .map(|i| {
            if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                s.value(i).to_string()
            } else {
                // StringViewArray path
                let s = col.as_any().downcast_ref::<arrow::array::StringViewArray>()
                    .expect("expected StringArray or StringViewArray");
                s.value(i).to_string()
            }
        })
        .collect();
    assert_eq!(name_strings, vec!["charlie", "dave", "eve"]);

    println!("DataFusion + RawObjectStore parquet test passed!");
}
