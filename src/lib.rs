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

use std::io::Result;
use std::time::Instant;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub mod tpcds;
pub mod tpch;

#[async_trait]
pub trait Tpc {
    fn generate(
        &self,
        scale: usize,
        partitions: usize,
        input_path: &str,
        output_path: &str,
    ) -> Result<()>;
    fn get_table_names(&self) -> Vec<&str>;
    fn get_schema(&self, table: &str) -> Schema;
    async fn convert_to_parquet(
        &self,
        input_path: &str,
        output_path: &str,
    ) -> datafusion::error::Result<()>;
}

pub async fn convert_tbl(
    input_path: &str,
    output_path: &str,
    options: CsvReadOptions<'_>,
    partitions: usize,
    file_format: &str,
    compression: &str,
    batch_size: usize,
) -> datafusion::error::Result<()> {
    println!(
        "Converting '{}' to {} files in directory '{}'",
        input_path, file_format, output_path
    );

    let start = Instant::now();

    let config = ExecutionConfig::new().with_batch_size(batch_size);
    let mut ctx = ExecutionContext::with_config(config);

    // build plan to read the TBL file
    let mut csv = ctx.read_csv(input_path, options).await?;

    // optionally, repartition the file
    if partitions > 1 {
        csv = csv.repartition(Partitioning::RoundRobinBatch(partitions))?
    }

    // create the physical plan
    let csv = csv.to_logical_plan();
    let csv = ctx.optimize(&csv)?;
    let csv = ctx.create_physical_plan(&csv).await?;

    match file_format {
        "csv" => ctx.write_csv(csv, output_path.to_string()).await?,
        "parquet" => {
            let compression = match compression {
                "none" => Compression::UNCOMPRESSED,
                "snappy" => Compression::SNAPPY,
                "brotli" => Compression::BROTLI,
                "gzip" => Compression::GZIP,
                "lz4" => Compression::LZ4,
                "lz0" => Compression::LZO,
                "zstd" => Compression::ZSTD,
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Invalid compression format: {}",
                        other
                    )))
                }
            };
            let props = WriterProperties::builder()
                .set_compression(compression)
                .build();

            ctx.write_parquet(csv, output_path.to_string(), Some(props))
                .await?
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Invalid output format: {}",
                other
            )))
        }
    }
    println!("Conversion completed in {} ms", start.elapsed().as_millis());

    Ok(())
}
