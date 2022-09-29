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
use tpctools::{convert_to_parquet, Tpc};

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

            if !opt.generator_path.exists() {
                panic!(
                    "generator path does not exist: {}",
                    opt.generator_path.display()
                )
            }

            if !opt.output.exists() {
                panic!("output path does not exist: {}", opt.output.display())
            }

            let generator_path = format!("{}", opt.generator_path.display());
            let output_path_str = format!("{}", opt.output.display());

            let tpc = create_benchmark(&opt.benchmark);

            tpc.generate(scale, partitions, &generator_path, &output_path_str)?;
        }
        Opt::Convert(opt) => {
            let tpc = create_benchmark(&opt.benchmark);
            match convert_to_parquet(
                tpc.as_ref(),
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

fn create_benchmark(name: &str) -> Box<dyn Tpc> {
    match name {
        "tpcds" | "tpc-ds" => Box::new(TpcDs::new()),
        "tpch" | "tpc-h" => Box::new(TpcH::new()),
        _ => panic!("invalid benchmark name"),
    }
}
