pub use arrow_array;
pub use arrow_ipc;
pub use arrow_schema;

pub use serde_json;

use std::collections::HashMap;
use std::io;

use io::BufWriter;
use io::Write;

use arrow_array::RecordBatch;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;

use arrow_ipc::writer::StreamWriter;

pub fn write_all<W, I>(mut wtr: StreamWriter<BufWriter<W>>, rows: I) -> Result<(), io::Error>
where
    W: Write,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    for rb in rows {
        let rbat: RecordBatch = rb?;
        wtr.write(&rbat).map_err(io::Error::other)?;
        wtr.flush().map_err(io::Error::other)?;
    }
    wtr.finish().map_err(io::Error::other)?;
    Ok(())
}

pub fn rows2writer<I, W>(rows: I, mut wtr: W, sch: &Schema) -> Result<(), io::Error>
where
    W: Write,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    let swtr = StreamWriter::try_new_buffered(&mut wtr, sch).map_err(io::Error::other)?;
    write_all(swtr, rows)?;
    wtr.flush()
}

pub fn rows2stdout<I>(rows: I, sch: &Schema) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    rows2writer(rows, io::stdout().lock(), sch)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BasicField {
    pub name: String,
    pub dtyp: DataType,
    pub null: bool,
}

impl From<BasicField> for Field {
    fn from(b: BasicField) -> Self {
        Field::new(b.name, b.dtyp, b.null)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct BasicSchema {
    pub fields: Vec<BasicField>,
    pub metadata: HashMap<String, String>,
}

impl From<BasicSchema> for Schema {
    fn from(b: BasicSchema) -> Self {
        let v: Vec<Field> = b.fields.into_iter().map(|f| f.into()).collect();
        Self {
            fields: v.into(),
            metadata: b.metadata,
        }
    }
}

impl BasicSchema {
    pub fn from_json(js_bytes: &[u8]) -> Result<Self, io::Error> {
        serde_json::from_slice(js_bytes).map_err(io::Error::other)
    }
}
