# TPC Tools

Tools for generating TPC-* data sets. Only TPC-DS is supported so far. 

## TPC-DS

Install dependencies.

```bash
sudo apt install gcc make flex bison byacc git
git clone https://github.com/databricks/tpcds-kit.git
```

Generate data.

```bash
cargo run --release -- --scale 1000 --partitions 48 --output /mnt/bigdata/tpcds/tbl-sf1000/ --tpcdskit-path ~/git/tpcds-kit/tools/
```

Example output.

```
Generated TPC-DS data at scale factor 1000 with 48 partitions in: 6247.155671938s
```
