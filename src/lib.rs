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
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::error::DataFusionError;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use futures::{FutureExt, StreamExt};

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

    fn get_table_ext(&self) -> &str;

    fn get_schema(&self, table: &str) -> Schema;
}

pub async fn convert_to_parquet(
    benchmark: &dyn Tpc,
    input_path: &str,
    output_path: &str,
) -> datafusion::error::Result<()> {
    for table in benchmark.get_table_names() {
        let schema = benchmark.get_schema(table);

        let file_ext = format!(".{}", benchmark.get_table_ext());
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .file_extension(&file_ext);

        let path = format!("{}/{}.{}", input_path, table, benchmark.get_table_ext());
        let path = Path::new(&path);
        if !path.exists() {
            panic!("path does not exist: {:?}", path);
        }

        // create output dir
        let output_dir_name = format!("{}/{}.parquet", output_path, table);
        let output_dir = Path::new(&output_dir_name);
        if output_dir.exists() {
            panic!("output dir already exists: {}", output_dir.display());
        }
        println!("Creating directory: {}", output_dir.display());
        fs::create_dir(&output_dir).unwrap();

        let files = fs::read_dir(path).unwrap();
        let mut file_vec = vec![];
        for file in files {
            let file = file?;
            file_vec.push(file);
        }

        // TODO make async again
        // let x = futures::stream::iter(file_vec.iter().map(|file| {

        let mut part = 0;
        for file in &file_vec {
            let stub = file.file_name().to_str().unwrap().to_owned();
            let stub = &stub[0..stub.len() - 4]; // remove .dat or .tbl
                                                 // write to temp dir that will contain nested dirs
                                                 // example: /tmp/nation-temp.parquet/part-1.parquet/part-0.parquet
            let output_parts_dir = format!("{}/{}-temp.parquet", output_dir.display(), stub);
            println!("Writing {}", output_parts_dir);
            let options = options.clone();
            // async move {
            convert_tbl(
                &file.path(),
                &output_parts_dir,
                &options,
                "parquet",
                "snappy",
                8192,
            )
            .await?;
            // }

            let paths = fs::read_dir(&output_parts_dir).unwrap();
            for path in paths {
                let path = path.unwrap();
                let path = format!("{}", path.path().display());
                let dest_file = format!("{}/part-{}.parquet", output_dir.display(), part);
                part += 1;
                println!("Moving {} to {}", path, dest_file);
                fs::rename(&path, &dest_file).unwrap();
            }
        }

        // }))
        // .buffer_unordered(3)
        // .map(|r| println!("finished request: {:?}", r))
        // .collect::<Vec<_>>();
    }

    Ok(())
}

pub async fn convert_tbl(
    input_path: &Path,
    output_filename: &str,
    options: &CsvReadOptions<'_>,
    file_format: &str,
    compression: &str,
    batch_size: usize,
) -> datafusion::error::Result<()> {
    println!(
        "Converting '{}' to {}",
        input_path.display(),
        output_filename
    );

    let start = Instant::now();

    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = SessionContext::with_config(config);

    // build plan to read the TBL file
    let csv_filename = format!("{}", input_path.display());
    let csv = ctx.read_csv(&csv_filename, options.clone()).await?;

    // create the physical plan
    let csv = csv.to_logical_plan()?;
    let csv = ctx.create_physical_plan(&csv).await?;

    match file_format {
        "csv" => ctx.write_csv(csv, output_filename.to_string()).await?,
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

            ctx.write_parquet(csv, output_filename.to_string(), Some(props))
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
