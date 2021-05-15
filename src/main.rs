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
use std::path::PathBuf;

use structopt::StructOpt;

use tpctools::tpcds::TpcDs;
use tpctools::tpch::TpcH;
use tpctools::Tpc;

#[derive(Debug, StructOpt)]
struct GenerateOpt {
    /// TPC benchmark to use (tpcds or tpch)
    #[structopt(short, long)]
    benchmark: String,

    /// Scale factor
    #[structopt(short, long)]
    scale: usize,

    /// Number of partitions to generate in parallel
    #[structopt(short, long)]
    partitions: usize,

    /// Path to tpcds-kit
    #[structopt(short, long, parse(from_os_str))]
    generator_path: PathBuf,

    /// Output path
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,
}

#[derive(Debug, StructOpt)]
struct ConvertOpt {
    /// TPC benchmark to use (tpcds or tpch)
    #[structopt(short, long)]
    benchmark: String,

    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,

    /// Compression to use when writing Parquet files
    #[structopt(short = "c", long = "compression", default_value = "snappy")]
    compression: String,

    /// Number of partitions to produce
    #[structopt(short = "p", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "tpctools",
    about = "Tools for generating and converting TPC data sets."
)]
enum Opt {
    Generate(GenerateOpt),
    Convert(ConvertOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    match Opt::from_args() {
        Opt::Generate(opt) => {
            let scale = opt.scale;
            let partitions = opt.partitions;
            let generator_path = opt.generator_path.as_path().to_str().unwrap().to_string();
            let output_path_str = opt.output.as_path().to_str().unwrap().to_string();

            let tpc: Box<dyn Tpc> = match opt.benchmark.as_str() {
                "tpcds" => Box::new(TpcDs::new()),
                "tpch" => Box::new(TpcH::new()),
                _ => panic!(),
            };

            tpc.generate(scale, partitions, &generator_path, &output_path_str)?;
        }
        Opt::Convert(opt) => {
            let tpc: Box<dyn Tpc> = match opt.benchmark.as_str() {
                "tpcds" => Box::new(TpcDs::new()),
                "tpch" => Box::new(TpcH::new()),
                _ => panic!(),
            };
            match tpc
                .convert_to_parquet(
                    opt.input_path.as_path().to_str().unwrap(),
                    opt.output_path.as_path().to_str().unwrap(),
                )
                .await
            {
                Ok(_) => {}
                Err(e) => println!("{:?}", e),
            }
        }
    }

    Ok(())
}
