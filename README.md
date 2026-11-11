# TSseek: Regular Expression-Based Search for Distributed Time Series Datasets

TSseek answers pattern queries over large time-series collections. A query can constrain value ranges, trends, step sizes, lengths, positions, and the order of and gaps between patterns. TSseek supports **whole matching**, where the whole series must match, and **subsequence matching**, where windows of a series must match. All answers are exact.

- **Offline.** Each series is split into ε-bounded line segments. The segments are stored in PostgreSQL/PostGIS tables partitioned over two database servers, together with per-cell segment counts on a position–value grid.
- **Online.** A query is rewritten into index probes, and the grid statistics choose which probes to run. The index lookup returns candidate series. Spark then retrieves the candidates' raw readings and checks the complete query.

## Repository layout

```
pom.xml                                  Maven build (Scala 2.13, Spark 3.5.0)
run_query.sh                             Runs one query (whole or subsequence matching)
sample_data/refit_sample.csv             100 REFIT series in the input format
src/main/scala/org/example/bigdata/
  TSseek_Config.scala                    All settings, read from TSSEEK_* environment variables
  TSseek_PrintConfig.scala               Prints the resolved settings
  TSseek_00_DataDistributor.scala        Loads the raw series from HDFS into PostgreSQL (used for retrieval)
  TSseek_01_Preprocessing.scala          Segmentation, grid statistics, segment tables and their indexes
  TSseek_02_BatchInsertion.scala         Loads the segment files into PostgreSQL on each database server
  TSseek_03_QueryLanguage.scala          Shared lexer of the query language
  TSseek_03_QueryWindowParser.scala      Whole matching: query rewriting and probe planning
  TSseek_03_SubseqQueryParser.scala      Subsequence matching: index conditions per sub-pattern
  TSseek_03_IndexContract.scala          Grid, probe windows and filters shared by indexing and querying
  TSseek_03_CostBasedDecision.scala      Grid-count cost estimates
  TSseek_04_QueryProc_Subseq.scala       Query processing for both matching modes (lookup, retrieval, refinement)
  TSseek_05_RetrievalRefinement.scala    Candidate retrieval and exact refinement for whole matching
  TSseek_05_SubseqDFA.scala              Exact refinement for subsequence matching
```

## Datasets

| Dataset | Source |
|---|---|
| REFIT | Electrical load of UK households (W). D. Murray, L. Stankovic, V. Stankovic, *Scientific Data* 4:160122, 2017. https://doi.org/10.1038/sdata.2016.122 |
| ECG | A Large-scale 12-lead Electrocardiogram Database for Arrhythmia Study (mV). https://physionet.org/content/ecg-arrhythmia/1.0.0/ |
| TSBS | Time Series Benchmark Suite, fuel consumption attribute. https://github.com/timescale/tsbs |
| Random Walk | Synthetic z-normalised random walks |

Each series has 128 readings by default (`TSSEEK_SERIES_LENGTH` changes this).

## Data format

Each input file is a CSV file with one series per line:
```
<time_series_id>;<MULTIPOINT geometry>
```

Example record from REFIT (128 readings, in W):
```
6;MULTIPOINT((0 423), (1 419), (2 423), ..., (127 352))
```

Each point is `(position value)`. `sample_data/refit_sample.csv` holds 100 REFIT series, each 128 consecutive readings of one household's aggregate power.

There is one input file per table, with 625,000 series each:

| Size | Files and tables |
|---|---|
| 25M | 40 |
| 50M | 80 |
| 100M | 160 |
| 200M | 320 |

Input locations on HDFS:
- **REFIT:** set `TSSEEK_REFIT_INPUT_HDFS` to the directory and `TSSEEK_REFIT_FILE_TEMPLATE` to the file name with `%d` for the file number (1..N), e.g. `time_series_25M_%d.csv`.
- **Other datasets:** fixed directories under `/user/YOUR_USERNAME/input/`, set in `TSseek_Config.scala`, `TSseek_00_DataDistributor.scala` and `TSseek_01_Preprocessing.scala`. TSBS and Random Walk are set up for 25M only.

## Configuration

### 1. Cluster settings

The code expects two machines. Each runs a Spark worker and a PostgreSQL server, and the first also runs the Spark master. Replace these placeholders in the `.scala` files (`grep -rn YOUR_ src/` lists them all):

| Placeholder | Replace with |
|---|---|
| `YOUR_PASSWORD_HERE` | PostgreSQL password of user `postgres` |
| `YOUR_SPARK_MASTER_HOST` | Host of the Spark master, which also runs the first PostgreSQL server |
| `YOUR_DB_SERVER2_HOST` | Host of the second PostgreSQL server |
| `YOUR_DB_SERVER1_IP`, `YOUR_DB_SERVER2_IP` | IP addresses of the two servers |
| `YOUR_USERNAME` | Cluster user (HDFS `/user/YOUR_USERNAME/`, local `/home/YOUR_USERNAME/`) |
| `shark1local` | HDFS namenode host |

The code labels the two database servers `shark1` and `shark2`; keep these labels.

### 2. Run settings

All programs read their settings from `TSSEEK_*` environment variables on the Spark driver. Nothing in the source needs editing. `TSseek_PrintConfig` prints the resolved values.

| Variable | Values (default) |
|---|---|
| `TSSEEK_DATASET` | `REFIT`, `ECG`, `TSBS`, `RANDOMWALK` (`ECG`) |
| `TSSEEK_DATASET_SIZE` | `25m`, `50m`, `100m`, `200m` (`25m`) |
| `TSSEEK_SERIES_LENGTH` | readings per series (`128`) |
| `TSSEEK_ALPHA` | ε = α × span90 (`0.6`) |
| `TSSEEK_<DATASET>_SPAN90`, `_GRID_W`, `_GRID_H`, `_Y_MIN`, `_Y_MAX` | index parameters: span90, grid cell width (readings) and height (value), value extent; required for REFIT |
| `TSSEEK_MATCHING_TYPE` | `WHOLE_SEQUENCE`, `SUBSEQUENCE` (`SUBSEQUENCE`) |
| `TSSEEK_QUERY_PATTERN` | the query (see [Query language](#query-language)) |
| `TSSEEK_SUBSEQUENCE_CASE` | the subsequence query class (see the table below) |
| `TSSEEK_REWRITE_RULES` | `none`, `backward`, `backward,tighten`, `backward,superset`, `backward,conflict`, `all` (`backward`) |
| `TSSEEK_SLOPE_FILTER` | `safe`, `off` (`safe`) |
| `TSSEEK_STEP_FILTER` | `true`, `false` (`false`) |
| `TSSEEK_MULTI_PATTERN_STRATEGY` | `COMPOSITE`, `INTERSECT_ALL`, `BEST_ONLY`, `CASCADE` (`CASCADE`) |

Settings for REFIT (ε = 65 W; the same index settings at 25M, 50M, 100M and 200M):

```bash
export TSSEEK_SERIES_LENGTH=128 TSSEEK_SLOPE_FILTER=safe TSSEEK_STEP_FILTER=false \
       TSSEEK_LEGACY_FLUCTUATION_FILTER=false TSSEEK_MULTI_PATTERN_STRATEGY=COMPOSITE TSSEEK_REWRITE_RULES=all
export TSSEEK_DATASET=REFIT TSSEEK_DATASET_SIZE=25m TSSEEK_ALPHA=0.031816 TSSEEK_REFIT_SPAN90=2043 \
       TSSEEK_REFIT_GRID_W=8 TSSEEK_REFIT_GRID_H=65 TSSEEK_REFIT_Y_MIN=-65 TSSEEK_REFIT_Y_MAX=70005
export TSSEEK_REFIT_INPUT_HDFS=hdfs://shark1local:9000/user/YOUR_USERNAME/input/refit/<input directory> \
       TSSEEK_REFIT_FILE_TEMPLATE='time_series_25M_%d.csv'
```

The other datasets use the same variables with their own prefix (`TSSEEK_TSBS_*`, `TSSEEK_ECG_*`, `TSSEEK_RANDOMWALK_*`).

Preprocessing stores the index parameters with the index. Query processing refuses to run if its grid differs from the index's or its ε is smaller than the index's.

## Running the pipeline

### Step 1: Build the JAR
```bash
mvn clean package
```

The JAR with dependencies is written to:
```
target/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Copy it to the cluster and set `JAR` to its path. Set the dataset's variables (above) in the shell before each step. In the names below, `<prefix>` is the dataset and size, e.g. `refit_25m`.

### Step 2: Load the raw series into PostgreSQL
```bash
spark-submit --master spark://YOUR_SPARK_MASTER_HOST:7077 --deploy-mode client \
  --class org.example.bigdata.TSseek_00_DataDistributor "$JAR"
```

This creates the tables `<prefix>_og_ts_table1..N` with a B-tree index on the series ID, split between the two servers. Refinement retrieves candidate series from them.

### Step 3: Preprocessing (segmentation and grid statistics)
```bash
spark-submit --master spark://YOUR_SPARK_MASTER_HOST:7077 --deploy-mode client \
  --driver-memory 128g --conf spark.cores.max=88 \
  --class org.example.bigdata.TSseek_01_Preprocessing "$JAR"
```

This step:
- writes the segments as CSV files to `/home/YOUR_USERNAME/results/<prefix>_segments/` on each server;
- creates the indexed segment tables `<prefix>_segments_table1..N`;
- stores the grid counts, the index parameters and the table-to-server map in HDFS `/user/YOUR_USERNAME/output/<prefix>_mapping/`.

### Step 4: Batch insertion
Run on each database server, with the same `TSSEEK_*` variables:
```bash
java -cp "$JAR" org.example.bigdata.TSseek_02_BatchInsertion
```

Then run `ANALYZE` on every `<prefix>_og_ts_table*` and `<prefix>_segments_table*` table.

### Step 5: Queries
```bash
./run_query.sh WHOLE_SEQUENCE '<query>'
./run_query.sh SUBSEQUENCE <SUBSEQUENCE_CASE> '<query>'
```

One Spark job runs the index lookup, then candidate retrieval and exact refinement. The driver log reports:
- the index-lookup time (`PHASE_TIMING stage1_sql`) and the candidates per table;
- the retrieval and refinement time (`PHASE_TIMING stage2_wall`);
- the number of matching series.

The matching series IDs are written to HDFS under `/user/YOUR_USERNAME/output/`. `DRY_RUN=1 ./run_query.sh ...` prints the command without running it.

## Query language

### Whole matching

A query is a comma-separated sequence of constructs that covers the series' positions in order. Positions left unspecified at the end are wildcards.

| Construct | Syntax | Meaning |
|---|---|---|
| Singleton | `v` | the reading equals `v` |
| Range | `[a, b]`; also `(a, b)`, `[a, b)`, `(a, b]` | the reading lies in the range |
| Pattern | `\| [a, b], <d>, m, {n} \|` or `{n1, n2}` | `n` readings (or `n1` to `n2`) in `[a, b]` whose consecutive steps follow direction `d` with step magnitude `m` |
| Wildcard | `(*, n)`, `(*, [a, b])`, `[*]`, `WC([*])` | `n`, `a` to `b`, or any number of unconstrained readings |

- **Directions:** `<+>` increasing, `<->` decreasing, `<=>` flat, `<~>` any.
- **Step magnitude:** `**` for any, `*s*` for an exact step size, or `[s1, s2]` for a step-size range.
- **Threshold:** an optional trailing `Threshold: τ` (default 0) is an absolute tolerance on consecutive-reading predicates. A flat Pattern allows |x<sub>j+1</sub> − x<sub>j</sub>| ≤ τ, and step bounds widen by τ.

Example (REFIT):
```
| [333, 729], <~>, **, {32} |, | [263, 864], <~>, **, {30, 34} |, | [263, 625], <~>, **, {32} |, | [466, 486], <=>, **, {30, 34} |
```

### Subsequence matching

A query is one or more Pattern blocks, each matching a contiguous window of the series:
- **Start of a block:** free by default. `@s` after a block fixes its start at position `s`, and `@[a, b]` bounds it.
- **Between blocks:** `, #n#,` for exactly `n` readings between the windows (`#0#` = adjacent), `, #a-b#,` for `a` to `b` readings, and `, ....,` for any gap. Blocks occur in the written order.
- **Any order:** the prefix `UNORDERED:` lets the blocks occur in any order; the blocks are then separated by `....`.
- **Threshold:** a trailing `Threshold: τ`, as in whole matching.

`TSSEEK_SUBSEQUENCE_CASE` selects the index-lookup plan and must match the query's form; the case names are listed in `TSseek_Config.scala`. Two examples on REFIT:

```bash
# 5 increasing readings in [3200, 4200] W, starting at position 21
./run_query.sh SUBSEQUENCE FIXED_START_FIXED_LEN '| [3200, 4200], <+>, **, {5} | @21 Threshold: 0'

# 4 increasing readings, then after a gap of 3 to 8 readings, 4 decreasing readings, all in [3200, 4200] W
./run_query.sh SUBSEQUENCE MULTI_RANGE_GAP '| [3200, 4200], <+>, **, {4} |, #3-8#, | [3200, 4200], <->, **, {4} | Threshold: 0'
```

## Requirements

- Apache Spark 3.5.0 (Scala 2.13 build)
- Hadoop 3.3.6 (HDFS)
- PostgreSQL 15 with PostGIS 3.3
- Java 17
- Maven 3.6 or later
