#![feature(test)]
extern crate locustdb;
extern crate test;
extern crate futures;

use std::path::Path;

use futures::executor::block_on;
use locustdb::LocustDB;


const DOWNLOAD_URL: &str = "https://www.dropbox.com/sh/4xm5vf1stnf7a0h/AADRRVLsqqzUNWEPzcKnGN_Pa?dl=0";
static mut DB: Option<LocustDB> = None;

fn db() -> &'static LocustDB {
    unsafe {
        match DB {
            Some(ref locustdb) => locustdb,
            None => {
                let locustdb = LocustDB::memory_only();
                let mut loads = Vec::new();
                for x in &["aa", "ab", "ac", "ad", "ae"] {
                    let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
                    if !Path::new(&path).exists() {
                        panic!("{} not found. Download dataset at {}", path, DOWNLOAD_URL);
                    }
                    loads.push(locustdb.load_csv(
                        locustdb::nyc_taxi_data::ingest_file(&path, "test")
                            .with_chunk_size(1 << 20)));
                }
                for l in loads {
                    let _ = block_on(l);
                }
                DB = Some(locustdb);
                DB.as_ref().unwrap()
            }
        }
    }
}

fn bench_query(b: &mut test::Bencher, query_str: &str) {
    let locustdb = db();
    b.iter(|| {
        let query = locustdb.run_query(query_str, false);
        block_on(query)
    });
}

#[bench]
fn count_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(1) from test;");
}

#[bench]
fn sum_total_amt_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, sum(total_amount) from test;");
}

#[bench]
fn select_passenger_count_sparse_filter(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime) from test where (passenger_count = 9) and (to_year(pickup_datetime) = 2014);");
}

#[bench]
fn select_star_limit_10000(b: &mut test::Bencher) {
    bench_query(b, "select * from test limit 10000;");
}

#[bench]
fn count_by_vendor_id_and_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select vendor_id, passenger_count, count(1) from test;");
}

#[bench]
fn q1_count_cab_type(b: &mut test::Bencher) {
    bench_query(b, "select cab_type, count(0) from test;");
}

#[bench]
fn q2_avg_total_amount_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(0), sum(total_amount) from test;");
}

#[bench]
fn q3_count_by_passenger_count_pickup_year(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), count(0) from test;");
}

#[bench]
fn q4_count_by_passenger_count_pickup_year_trip_distance(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from test;");
}
