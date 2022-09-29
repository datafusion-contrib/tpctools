# TPC Tools

Command-line tools for generating TPC-H and TPC-DS data sets in parallel and re-organizing the output files 
into directory structures that can be consumed by tools such as Apache Spark or Apache Arrow DataFusion/Ballista.  

## TPC-DS

Install dependencies.

```bash
sudo apt install gcc make flex bison byacc git
git clone https://github.com/databricks/tpcds-kit.git
```

Generate data.

```bash
mkdir /tmp/tpcds/sf1000

cargo run --release -- generate --benchmark tpcds \
  --scale 1000 \
  --partitions 48 \
  --generator-path ~/git/tpcds-kit/tools/ \
  --output /tmp/tpcds/sf1000/
```

Example output.

```
Generated TPC-DS data at scale factor 1000 with 48 partitions in: 6247.155671938s
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
  --partitions 1 \
  --generator-path ~/git/tpch-dbgen/ \
  --output /tmp/tpch/sf1
```

