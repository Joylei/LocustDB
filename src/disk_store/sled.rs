use byteorder::{BigEndian, ByteOrder};
use sled::Transactional;
use std::path::Path;
use std::str;
use std::sync::Arc;

use crate::disk_store::interface::*;
use crate::engine::data_types::EncodingType as Type;
use crate::mem_store::codec::CodecOp;
use crate::mem_store::column::{Column, DataSection, DataSource};
use crate::scheduler::inner_locustdb::InnerLocustDB;
use crate::unit_fmt::*;

pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let db = sled::open(path).unwrap();
        Self { db }
    }

    fn metadata(&self) -> sled::Result<sled::Tree> {
        self.db.open_tree(b"metadata")
    }

    fn partitions(&self) -> sled::Result<sled::Tree> {
        self.db.open_tree(b"partitions")
    }
}

impl DiskStore for SledStore {
    fn load_metadata(&self) -> Vec<PartitionMetadata> {
        let mut metadata = Vec::new();
        let tree = self.metadata().unwrap();
        for key_value in &tree {
            let (key, value) = key_value.unwrap();
            let partition_id = BigEndian::read_u64(&*key) as PartitionID;
            metadata.push(deserialize_meta_data(&*value, partition_id));
        }
        metadata
    }

    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column {
        let tree = self.partitions().unwrap();
        let data = tree
            .get(&column_key(partition, column_name))
            .unwrap()
            .unwrap();
        let (col, _) = decode_column(&*data);
        col
    }

    fn load_column_range(
        &self,
        start: PartitionID,
        end: PartitionID,
        column_name: &str,
        ldb: &InnerLocustDB,
    ) {
        let range_start: &[u8] = &column_key(start, column_name);
        let range_end: &[u8] = &column_key(end, column_name);

        let tree = self.partitions().unwrap();
        let iterator = tree.range(range_start..range_end);
        for key_value in iterator {
            let (key, value) = key_value.unwrap();
            let (key, _) = decode_column_key(&*key);
            if key.name != column_name || key.id > end {
                return;
            }
            let (col, _) = decode_column(&*value);
            ldb.restore(key.id, col);
        }
    }

    fn bulk_load(&self, ldb: &InnerLocustDB) {
        let tree = self.partitions().unwrap();
        let mut t = time::OffsetDateTime::now_utc();
        let mut size_total = 0;
        for key_value in &tree {
            let (key, value) = key_value.unwrap();
            let (key, _) = decode_column_key(&*key);
            let (col, _) = decode_column(&*value);
            let size = col.heap_size_of_children();
            let now = time::OffsetDateTime::now_utc();
            size_total += size;
            let elapsed = (now - t).whole_nanoseconds() as u64;
            if elapsed > 1_000_000_000 {
                debug!("restoring {}.{} {}", key.name, key.id, byte(size as f64));
                debug!(
                    "{}/s",
                    byte((1_000_000_000 * size_total as u64 / elapsed) as f64)
                );
                t = now;
                size_total = 0;
            }

            ldb.restore(key.id, col);
        }
    }

    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &[Arc<Column>]) {
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, partition as u64);
        let md = serialize_meta_data(tablename, columns);

        let metadata = self.metadata().unwrap();
        let partitions = self.partitions().unwrap();

        Transactional::<()>::transaction(&(&metadata, &partitions), |(metadata, partitions)| {
            metadata.insert(&key[..], &md[..]).unwrap();

            for column in columns {
                let key = column_key(partition, column.name());
                let mut data = vec![];
                encode_column(&column, &mut data);
                partitions.insert(&key[..], &data[..]).unwrap();
            }
            Ok(())
        })
        .unwrap();
    }
}

fn deserialize_meta_data(data: &[u8], partition_id: PartitionID) -> PartitionMetadata {
    let (mut meta, _) = decode_partition_metadata(data);
    meta.id = partition_id;
    meta
}

fn serialize_meta_data(tablename: &str, columns: &[Arc<Column>]) -> Vec<u8> {
    let mut buf = vec![];
    encode_str(tablename, &mut buf);
    columns.iter().for_each(|col| {
        encode_str(col.name(), &mut buf);
    });
    buf
}

fn column_key(partition: PartitionID, column_name: &str) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend(column_name.as_bytes());
    let pos = key.len();
    key.resize(pos + 8, 0);
    BigEndian::write_u64(&mut key[pos..], partition as u64);
    key
}

// -------------------------------------------------------------

// --- bool ---
fn decode_bool(buf: &[u8]) -> (bool, &[u8]) {
    let flag = buf[0];
    let v = match flag {
        0 => false,
        1 => true,
        _ => unreachable!(),
    };
    (v, &buf[1..])
}

fn encode_bool(val: bool, buf: &mut Vec<u8>) {
    let v = if val { 1 } else { 0 };
    buf.push(v);
}

// --- &str ---
fn decode_str(buf: &[u8]) -> (&str, &[u8]) {
    let str_len = BigEndian::read_u64(&buf[0..8]) as usize;
    let pos = str_len + 8;
    let str_bytes = &buf[8..pos];
    let text = str::from_utf8(str_bytes).unwrap();
    let remain = &buf[pos..];
    (text, remain)
}

fn encode_str(text: &str, buf: &mut Vec<u8>) {
    let str_len = text.len() as u64;
    let pos = buf.len();
    buf.resize(pos + 8, 0);
    {
        let buf = &mut buf[pos..];
        BigEndian::write_u64(buf, str_len);
    }
    buf.extend(text.as_bytes());
}

// --- Option<(i64,i64)> ---
fn decode_range(buf: &[u8]) -> (Option<(i64, i64)>, &[u8]) {
    let (flag, mut remain) = decode_bool(buf);
    let mut range = None;
    if flag {
        let start = BigEndian::read_i64(&remain[0..8]);
        let end = BigEndian::read_i64(&remain[8..16]);
        range = Some((start, end));
        remain = &remain[16..];
    }
    (range, remain)
}

fn encode_range(range: &Option<(i64, i64)>, buf: &mut Vec<u8>) {
    match range {
        Some((start, end)) => {
            encode_bool(true, buf);
            let pos = buf.len();
            buf.resize(pos + 16, 0);
            BigEndian::write_i64(&mut buf[pos..], *start);
            BigEndian::write_i64(&mut buf[pos + 8..], *end);
        }
        None => {
            encode_bool(false, buf);
        }
    }
}

// --- Type ---
fn decode_type(buf: &[u8]) -> (Type, &[u8]) {
    let flag = buf[0];
    let mut remain = &buf[1..];
    let encoding = match flag {
        0 => Type::Str,
        1 => Type::OptStr,
        2 => Type::I64,
        3 => Type::U8,
        4 => Type::U16,
        5 => Type::U32,
        6 => Type::U64,
        7 => Type::NullableStr,
        8 => Type::NullableI64,
        9 => Type::NullableU8,
        10 => Type::NullableU16,
        11 => Type::NullableU32,
        12 => Type::NullableU64,
        13 => Type::USize,
        14 => Type::Val,
        15 => Type::Null,
        16 => Type::ScalarI64,
        17 => Type::ScalarStr,
        18 => Type::ScalarString,
        19 => Type::ConstVal,
        20 => {
            let size = BigEndian::read_u64(remain);
            remain = &remain[8..];
            Type::ByteSlices(size as usize)
        }
        21 => Type::ValRows,
        22 => Type::Premerge,
        23 => Type::MergeOp,
        _ => panic!("invalid type"),
    };
    (encoding, remain)
}

fn encode_type(encoding: &Type, buf: &mut Vec<u8>) {
    match encoding {
        Type::Str => buf.push(0),
        Type::OptStr => buf.push(1),
        Type::I64 => buf.push(2),
        Type::U8 => buf.push(3),
        Type::U16 => buf.push(4),
        Type::U32 => buf.push(5),
        Type::U64 => buf.push(6),
        Type::NullableStr => buf.push(7),
        Type::NullableI64 => buf.push(8),
        Type::NullableU8 => buf.push(9),
        Type::NullableU16 => buf.push(10),
        Type::NullableU32 => buf.push(11),
        Type::NullableU64 => buf.push(12),
        Type::USize => buf.push(13),
        Type::Val => buf.push(14),
        Type::Null => buf.push(15),
        Type::ScalarI64 => buf.push(16),
        Type::ScalarStr => buf.push(17),
        Type::ScalarString => buf.push(18),
        Type::ConstVal => buf.push(19),
        Type::ByteSlices(size) => {
            buf.push(20);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_u64(&mut buf[pos..], *size as u64);
        }
        Type::ValRows => buf.push(21),
        Type::Premerge => buf.push(22),
        Type::MergeOp => buf.push(23),
    }
}

// --- DataSection ---
fn decode_data_section(buf: &[u8]) -> (DataSection, &[u8]) {
    let flag = buf[0];
    let remain = &buf[1..];
    match flag {
        0 => {
            let (data, remain) = decode_vec(remain, |buf| (buf[0], &buf[1..]));
            (DataSection::U8(data), remain)
        }
        1 => {
            let (data, remain) = decode_vec(remain, |buf| (BigEndian::read_u16(buf), &buf[2..]));
            (DataSection::U16(data), remain)
        }
        2 => {
            let (data, remain) = decode_vec(remain, |buf| (BigEndian::read_u32(buf), &buf[4..]));
            (DataSection::U32(data), remain)
        }
        3 => {
            let (data, remain) = decode_vec(remain, |buf| (BigEndian::read_u64(buf), &buf[8..]));
            (DataSection::U64(data), remain)
        }
        4 => {
            let (data, remain) = decode_vec(remain, |buf| (BigEndian::read_i64(buf), &buf[8..]));
            (DataSection::I64(data), remain)
        }
        5 => {
            let num = BigEndian::read_u64(&remain[0..8]) as usize;
            (DataSection::Null(num), &remain[8..])
        }
        _ => unreachable!(),
    }
}

fn encode_data_section(section: &DataSection, buf: &mut Vec<u8>) {
    match section {
        DataSection::U8(data) => {
            buf.push(0);
            encode_slice(&data, buf, |v, buf| buf.push(*v));
        }
        DataSection::U16(data) => {
            buf.push(1);
            encode_slice(&data, buf, |v, buf| {
                let pos = buf.len();
                buf.resize(pos + 2, 0);
                BigEndian::write_u16(&mut buf[pos..], *v);
            });
        }
        DataSection::U32(data) => {
            buf.push(2);
            encode_slice(&data, buf, |v, buf| {
                let pos = buf.len();
                buf.resize(pos + 4, 0);
                BigEndian::write_u32(&mut buf[pos..], *v);
            });
        }
        DataSection::U64(data) => {
            buf.push(3);
            encode_slice(&data, buf, |v, buf| {
                let pos = buf.len();
                buf.resize(pos + 8, 0);
                BigEndian::write_u64(&mut buf[pos..], *v);
            });
        }
        DataSection::I64(data) => {
            buf.push(4);
            encode_slice(&data, buf, |v, buf| {
                let pos = buf.len();
                buf.resize(pos + 8, 0);
                BigEndian::write_i64(&mut buf[pos..], *v);
            });
        }
        DataSection::Null(v) => {
            buf.push(5);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_u64(&mut buf[pos..], *v as u64);
        } //_ => unreachable!(),
    }
}

// --- CodecOp ---
fn decode_codec_op(buf: &[u8]) -> (CodecOp, &[u8]) {
    let flag = buf[0];
    let mut remain = &buf[1..];
    let op = match flag {
        0 => CodecOp::Nullable,
        1 => {
            let (encoding, buf) = decode_type(remain);
            let num = BigEndian::read_i64(buf);
            remain = &buf[8..];
            CodecOp::Add(encoding, num)
        }
        2 => {
            let (encoding, buf) = decode_type(remain);
            remain = buf;
            CodecOp::Delta(encoding)
        }
        3 => {
            let (encoding, buf) = decode_type(remain);
            remain = buf;
            CodecOp::ToI64(encoding)
        }
        4 => {
            let num = BigEndian::read_u64(remain) as usize;
            remain = &remain[8..];
            CodecOp::PushDataSection(num)
        }
        5 => {
            let (encoding, buf) = decode_type(remain);
            remain = buf;
            CodecOp::DictLookup(encoding)
        }
        6 => {
            let (encoding, buf) = decode_type(remain);
            let num = BigEndian::read_u64(buf) as usize;
            remain = &buf[8..];
            CodecOp::LZ4(encoding, num)
        }
        7 => CodecOp::UnpackStrings,
        8 => {
            let (flag, buf) = decode_bool(remain);
            let num = BigEndian::read_u64(buf) as usize;
            remain = &buf[8..];
            CodecOp::UnhexpackStrings(flag, num)
        }
        _ => unreachable!(),
    };
    (op, remain)
}

fn encode_codec_op(op: &CodecOp, buf: &mut Vec<u8>) {
    match op {
        CodecOp::Nullable => {
            buf.push(0);
        }
        CodecOp::Add(encoding, num) => {
            buf.push(1);
            encode_type(encoding, buf);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_i64(&mut buf[pos..], *num);
        }
        CodecOp::Delta(encoding) => {
            buf.push(2);
            encode_type(encoding, buf);
        }
        CodecOp::ToI64(encoding) => {
            buf.push(3);
            encode_type(encoding, buf);
        }
        CodecOp::PushDataSection(num) => {
            buf.push(4);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_u64(&mut buf[pos..], *num as u64);
        }
        CodecOp::DictLookup(encoding) => {
            buf.push(5);
            encode_type(encoding, buf);
        }
        CodecOp::LZ4(encoding, num) => {
            buf.push(6);
            encode_type(encoding, buf);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_u64(&mut buf[pos..], *num as u64);
        }
        CodecOp::UnpackStrings => buf.push(7),
        CodecOp::UnhexpackStrings(flag, num) => {
            buf.push(8);
            encode_bool(*flag, buf);
            let pos = buf.len();
            buf.resize(pos + 8, 0);
            BigEndian::write_u64(&mut buf[pos..], *num as u64);
        }
        _ => unreachable!(),
    }
}

// --- Vec --
fn decode_vec<T, F>(buf: &[u8], f: F) -> (Vec<T>, &[u8])
where
    F: Fn(&[u8]) -> (T, &[u8]),
{
    let count = BigEndian::read_u64(buf) as usize;
    decode_vec_with_count(&buf[8..], count, f)
}

fn decode_vec_with_count<T, F>(buf: &[u8], count: usize, f: F) -> (Vec<T>, &[u8])
where
    F: Fn(&[u8]) -> (T, &[u8]),
{
    let mut remain = buf;
    let mut res = vec![];
    for _ in 0..count {
        let (item, buf) = f(remain);
        res.push(item);
        remain = buf;
    }
    (res, remain)
}

fn encode_slice<T, F>(slice: &[T], buf: &mut Vec<u8>, f: F)
where
    F: Fn(&T, &mut Vec<u8>),
{
    let pos = buf.len();
    buf.resize(pos + 8, 0);
    BigEndian::write_u64(&mut buf[pos..], slice.len() as u64);

    for item in slice.iter() {
        f(item, buf);
    }
}

// --- Column ---
fn decode_column(buf: &[u8]) -> (Column, &[u8]) {
    let (name, remain) = decode_str(&buf);
    let (range, remain) = decode_range(remain);
    let (codec, remain) = decode_vec(remain, |buf| decode_codec_op(buf));
    let (data, remain) = decode_vec(remain, |buf| decode_data_section(buf));
    (Column::new(name, data.len(), range, codec, data), remain)
}

fn encode_column(col: &Column, buf: &mut Vec<u8>) {
    encode_str(col.name(), buf);
    encode_range(&col.range(), buf);
    encode_slice(col.codec().ops(), buf, |op, buf| encode_codec_op(op, buf));
    encode_slice(col.data(), buf, |section, buf| {
        encode_data_section(section, buf)
    });
}

// --- ColumnKey ---
struct ColumnKey<'a> {
    id: PartitionID,
    name: &'a str,
}

fn decode_column_key(buf: &[u8]) -> (ColumnKey<'_>, &[u8]) {
    let pos = buf.len() - 8;
    let id = BigEndian::read_u64(&buf[pos..]);
    let name = str::from_utf8(&buf[0..pos]).unwrap();
    (ColumnKey { id, name }, Default::default())
}

// impl<'a> Encode for ColumnKey<'a> {
//     fn encode(&self, buf: &mut Vec<u8>) {
//         buf.extend(self.name.as_bytes());

//         buf.resize(buf.len() + 8, 0);
//         BigEndian::write_u64(&mut buf[buf.len() - 8..], self.partition as u64);
//         key
//     }
// }

// --- PartitionMetadata ---
fn decode_partition_metadata(buf: &[u8]) -> (PartitionMetadata, &[u8]) {
    // store format: [str_len + str_bytes]*
    let (tablename, mut remain) = decode_str(buf);
    let mut columns = vec![];
    while remain.len() > 0 {
        let (name, buf) = decode_str(remain);
        columns.push(ColumnMetadata {
            name: name.to_string(),
            size_bytes: name.as_bytes().len(),
        });
        remain = buf;
    }

    (
        PartitionMetadata {
            id: 0,
            len: columns.len(),
            tablename: tablename.to_string(),
            columns,
        },
        remain,
    )
}

// fn encode_partition_metadata(partition: &PartitionMetadata, buf: &mut Vec<u8>) {
//     encode_str(partition.tablename.as_str(), buf);
//     partition.columns.iter().for_each(|col| {
//         encode_str(col.name.as_str(), buf);
//     });
// }
