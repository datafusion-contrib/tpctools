// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::io::Result;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::CsvReadOptions;

use crate::{convert_tbl, Tpc};

pub struct TpcH {}

impl TpcH {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Tpc for TpcH {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        generator_path: &str,
        output_path: &str,
    ) -> Result<()> {
        let mut handles = vec![];

        let start = Instant::now();

        if partitions == 1 {
            let generator_path = generator_path.to_owned();
            handles.push(thread::spawn(move || {
                let output = Command::new("./dbgen")
                    .current_dir(generator_path)
                    .arg("-f")
                    .arg("-s")
                    .arg(format!("{}", scale))
                    .output()
                    .expect("failed to generate data");
                println!("{:?}", output);
            }));
        } else {
            for i in 1..=partitions {
                let generator_path = generator_path.to_owned();
                handles.push(thread::spawn(move || {
                    let output = Command::new("./dbgen")
                        .current_dir(generator_path)
                        .arg("-f")
                        .arg("-s")
                        .arg(format!("{}", scale))
                        .arg("-C")
                        .arg(format!("{}", partitions))
                        .arg("-S")
                        .arg(format!("{}", i))
                        .output()
                        .expect("failed to generate data");
                    println!("{:?}", output);
                }));
            }
        }

        // wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        println!(
            "Generated TPC-H data at scale factor {} with {} partitions in: {:?}",
            scale, partitions, duration
        );

        let tables = [
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

        if !Path::new(&output_path).exists() {
            println!("Creating directory {}", output_path);
            fs::create_dir(&output_path)?;
        }

        for table in &tables {
            let output_dir = format!("{}/{}.tbl", output_path, table);
            if !Path::new(&output_dir).exists() {
                println!("Creating directory {}", output_dir);
                fs::create_dir(&output_dir)?;
            }

            let filename = format!("{}/{}.tbl", generator_path, table);
            let filename2 = format!("{}/part-0.dat", output_dir);
            if Path::new(&filename).exists() {
                println!("mv {} {}", filename, filename2);
                fs::rename(filename, filename2)?;
            }

            if partitions == 1 {
                let filename = format!("{}/{}.tbl", generator_path, table);
                let filename2 = format!("{}/part-0.tbl", output_dir);
                if Path::new(&filename).exists() {
                    println!("mv {} {}", filename, filename2);
                    fs::rename(filename, filename2)?;
                }
            } else {
                for i in 1..=partitions {
                    let filename = format!("{}/{}.tbl.{}", generator_path, table, i);
                    let filename2 = format!("{}/part-{}.tbl", output_dir, i);
                    if Path::new(&filename).exists() {
                        println!("mv {} {}", filename, filename2);
                        fs::rename(filename, filename2)?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn convert_to_parquet(
        &self,
        input_path: &str,
        output_path: &str,
    ) -> datafusion::error::Result<()> {
        for table in self.get_table_names() {
            let schema = self.get_schema(table);
            let options = CsvReadOptions::new()
                .schema(&schema)
                .delimiter(b'|')
                .file_extension(".tbl");

            let path = format!("{}/{}.tbl", input_path, table);
            let output_dir = format!("{}/{}.parquet", output_path, table);
            convert_tbl(&path, &output_dir, options, 1, "parquet", "snappy", 8192).await?;
        }

        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec![
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ]
    }

    fn get_schema(&self, table: &str) -> Schema {
        // note that the schema intentionally uses signed integers so that any generated Parquet
        // files can also be used to benchmark tools that only support signed integers, such as
        // Apache Spark

        match table {
            "part" => Schema::new(vec![
                Field::new("p_partkey", DataType::Int64, false),
                Field::new("p_name", DataType::Utf8, false),
                Field::new("p_mfgr", DataType::Utf8, false),
                Field::new("p_brand", DataType::Utf8, false),
                Field::new("p_type", DataType::Utf8, false),
                Field::new("p_size", DataType::Int32, false),
                Field::new("p_container", DataType::Utf8, false),
                Field::new("p_retailprice", DataType::Float64, false),
                Field::new("p_comment", DataType::Utf8, false),
            ]),

            "supplier" => Schema::new(vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_name", DataType::Utf8, false),
                Field::new("s_address", DataType::Utf8, false),
                Field::new("s_nationkey", DataType::Int64, false),
                Field::new("s_phone", DataType::Utf8, false),
                Field::new("s_acctbal", DataType::Float64, false),
                Field::new("s_comment", DataType::Utf8, false),
            ]),

            "partsupp" => Schema::new(vec![
                Field::new("ps_partkey", DataType::Int64, false),
                Field::new("ps_suppkey", DataType::Int64, false),
                Field::new("ps_availqty", DataType::Int32, false),
                Field::new("ps_supplycost", DataType::Float64, false),
                Field::new("ps_comment", DataType::Utf8, false),
            ]),

            "customer" => Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
                Field::new("c_address", DataType::Utf8, false),
                Field::new("c_nationkey", DataType::Int64, false),
                Field::new("c_phone", DataType::Utf8, false),
                Field::new("c_acctbal", DataType::Float64, false),
                Field::new("c_mktsegment", DataType::Utf8, false),
                Field::new("c_comment", DataType::Utf8, false),
            ]),

            "orders" => Schema::new(vec![
                Field::new("o_orderkey", DataType::Int64, false),
                Field::new("o_custkey", DataType::Int64, false),
                Field::new("o_orderstatus", DataType::Utf8, false),
                Field::new("o_totalprice", DataType::Float64, false),
                Field::new("o_orderdate", DataType::Date32, false),
                Field::new("o_orderpriority", DataType::Utf8, false),
                Field::new("o_clerk", DataType::Utf8, false),
                Field::new("o_shippriority", DataType::Int32, false),
                Field::new("o_comment", DataType::Utf8, false),
            ]),

            "lineitem" => Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int32, false),
                Field::new("l_quantity", DataType::Float64, false),
                Field::new("l_extendedprice", DataType::Float64, false),
                Field::new("l_discount", DataType::Float64, false),
                Field::new("l_tax", DataType::Float64, false),
                Field::new("l_returnflag", DataType::Utf8, false),
                Field::new("l_linestatus", DataType::Utf8, false),
                Field::new("l_shipdate", DataType::Date32, false),
                Field::new("l_commitdate", DataType::Date32, false),
                Field::new("l_receiptdate", DataType::Date32, false),
                Field::new("l_shipinstruct", DataType::Utf8, false),
                Field::new("l_shipmode", DataType::Utf8, false),
                Field::new("l_comment", DataType::Utf8, false),
            ]),

            "nation" => Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, false),
                Field::new("n_regionkey", DataType::Int64, false),
                Field::new("n_comment", DataType::Utf8, false),
            ]),

            "region" => Schema::new(vec![
                Field::new("r_regionkey", DataType::Int64, false),
                Field::new("r_name", DataType::Utf8, false),
                Field::new("r_comment", DataType::Utf8, false),
            ]),

            _ => unimplemented!(),
        }
    }
}
