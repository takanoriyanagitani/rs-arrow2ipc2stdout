use std::io;

use rs_arrow2ipc2stdout::arrow_array::RecordBatch;
use rs_arrow2ipc2stdout::arrow_array::record_batch;

use rs_arrow2ipc2stdout::arrow_schema;
use rs_arrow2ipc2stdout::arrow_schema::Schema;

use rs_arrow2ipc2stdout::BasicSchema;
use rs_arrow2ipc2stdout::rows2stdout;

fn sample_rows() -> impl Iterator<Item = Result<RecordBatch, io::Error>> {
    vec![
        record_batch!(
            ("timestamp_us", Int64, [0, 1, 2, 3, 4, 5, 6]),
            ("severity", Int8, [0, 1, 5, 9, 13, 17, 21]),
            ("status", Int16, [200, 404, 500, 200, 200, 404, 500]),
            ("body", Utf8, ["ok", "ng", "ng", "ok", "ok", "ng", "ng"])
        )
        .map_err(io::Error::other),
    ]
    .into_iter()
}

fn sample_schema_json() -> String {
    r#"{
        "fields": [
          {"name":"timestamp_us", "dtyp":"Int64", "null":false},
          {"name":"severity",     "dtyp":"Int8",  "null":false},
          {"name":"status",       "dtyp":"Int16", "null":false},
          {"name":"body",         "dtyp":"Utf8",  "null":false}
        ],
        "metadata": {}
    }"#
    .into()
}

fn sample_schema() -> Result<Schema, io::Error> {
    let sjson: String = sample_schema_json();
    let s: &[u8] = sjson.as_bytes();
    BasicSchema::from_json(s).map(|b| b.into())
}

fn main() -> Result<(), io::Error> {
    let s: Schema = sample_schema()?;
    let rows = sample_rows();
    rows2stdout(rows, &s)?;
    Ok(())
}
