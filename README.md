# TPC Tools

Command-line tools for invoking TPC-H and TPC-DS data generators in parallel and re-organizing the output files
into directory structures that can be consumed by tools such as Apache Spark or Apache Arrow DataFusion/Ballista.

Also supports converting the output to Parquet.

## TPC-DS

Install dependencies.

```bash
sudo apt install gcc make flex bison byacc git
```

Download data generator from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

```bash
cd /path/to/DSGen-software-code-3.2.0rc1/tools
make
```

Generate data.

```bash
mkdir /tmp/tpcds/sf1000

cargo run --release -- generate --benchmark tpcds \
  --scale 1000 \
  --partitions 48 \
  --generator-path /path/to/DSGen-software-code-3.2.0rc1/tools \
  --output /tmp/tpcds/sf1000/
```

Example output.

```
Generated TPC-DS data at scale factor 1000 with 48 partitions in: 6247.155671938s
```

Convert to Parquet

```bash
mkdir /tmp/tpcds/sf1000-parquet

cargo run --release -- convert --benchmark tpcds \
  --input /tmp/tpcds/sf1000/
  --output /tmp/tpcds/sf1000-parquet/
```

## TPC-H

Install dependencies.

```bash
git clone git@github.com:databricks/tpch-dbgen.git
cd tpch-dbgen
make
cd ..
```

Generate data.

```bash
mkdir /tmp/tpch

cargo run --release -- generate --benchmark tpch \
  --scale 1 \
  --partitions 2 \
  --generator-path ./tpch-dbgen/ \
  --output /tmp/tpch
```

Convert data to Parquet

```bash
mkdir /tmp/tpch-parquet

cargo run --release -- convert \
  --benchmark tpch \
  --input /tmp/tpch/ \
  --output /tmp/tpch-parquet/
```

# Legal Stuff

TPC-H is Copyright &copy; 1993-2022 Transaction Processing Performance Council. The full TPC-H specification in PDF
format can be found [here](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf)

TPC-DS is Copyright &copy; 2021 Transaction Processing Performance Council. The full TPC-DS specification in PDF
format can be found [here](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf)

TPC, TPC Benchmark, TPC-H, and TPC-DS are trademarks of the Transaction Processing Performance Council.
