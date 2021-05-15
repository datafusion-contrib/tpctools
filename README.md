# TPC Tools

Tools for generating TPC-H and TPC-DS data sets.  

## TPC-DS

Install dependencies.

```bash
sudo apt install gcc make flex bison byacc git
git clone https://github.com/databricks/tpcds-kit.git
```

Generate data.

```bash
cargo run --release -- generate --benchmark tpcds --scale 1000 --partitions 48 --generator_path ~/git/tpcds-kit/tools/ --output /mnt/bigdata/tpcds/tbl-sf1000/
```

Example output.

```
Generated TPC-DS data at scale factor 1000 with 48 partitions in: 6247.155671938s
```

## TPC-H

Install dependencies.

```bash
git clone // git@github.com:databricks/tpch-dbgen.git
```

Generate data.

```bash
cargo run --release -- generate --benchmark tpch --scale 100 --partitions 24 --generator_path ~/git/tpch-dbgen/ --output /mnt/bigdata/tpcds/tbl-sf1000/
```

