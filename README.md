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
```

Generate data.

```bash
cargo run --release -- generate --benchmark tpch \
  --scale 1 \
  --partitions 2 \
  --generator-path ~/git/tpch-dbgen/ \
  --output /tmp/tpch/sf1
```

