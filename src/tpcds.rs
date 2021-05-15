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

use arrow::datatypes::Schema;
use async_trait::async_trait;

use crate::Tpc;

pub struct TpcDs {}

impl TpcDs {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Tpc for TpcDs {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        generator_path: &str,
        output_path: &str,
    ) -> Result<()> {
        let mut handles = vec![];

        let start = Instant::now();

        for i in 1..=partitions {
            let generator_path = generator_path.to_owned();
            let output_path = output_path.to_owned();
            let scale = scale;
            let partitions = partitions;
            handles.push(thread::spawn(move || {
                let output = Command::new("./dsdgen")
                    .current_dir(generator_path)
                    .arg("-FORCE")
                    .arg("-DIR")
                    .arg(output_path)
                    .arg("-SCALE")
                    .arg(format!("{}", scale))
                    .arg("-CHILD")
                    .arg(format!("{}", i))
                    .arg("-PARALLEL")
                    .arg(format!("{}", partitions))
                    .output()
                    .expect("failed to generate data");

                println!("{:?}", output);
            }));
        }

        // wait for all threads to finish
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        println!(
            "Generated TPC-DS data at scale factor {} with {} partitions in: {:?}",
            scale, partitions, duration
        );

        let tables = self.get_table_names();

        for table in &tables {
            let output_dir = format!("{}/{}", output_path, table);
            if !Path::new(&output_dir).exists() {
                println!("Creating directory {}", output_dir);
                fs::create_dir(&output_dir)?;
            }
            for i in 1..=partitions {
                let filename = format!("{}/{}_{}_{}.dat", output_path, table, i, partitions);
                let filename2 = format!("{}/part-{}.dat", output_dir, i);
                if Path::new(&filename).exists() {
                    println!("mv {} {}", filename, filename2);
                    fs::rename(filename, filename2)?;
                }
            }
        }

        Ok(())
    }

    fn get_table_names(&self) -> Vec<&str> {
        vec![
            "call_center",
            "catalog_page",
            "catalog_sales",
            "catalog_returns",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "income_band",
            "household_demographics",
            "inventory",
            "store",
            "ship_mode",
            "reason",
            "promotion",
            "item",
            "store_sales",
            "store_returns",
            "web_page",
            "warehouse",
            "time_dim",
            "web_site",
            "web_sales",
            "web_returns",
        ]
    }

    fn get_schema(&self, _table: &str) -> Schema {
        todo!()
    }

    async fn convert_to_parquet(
        &self,
        _input_path: &str,
        _output_path: &str,
    ) -> datafusion::error::Result<()> {
        todo!()
    }
}
