#!/usr/bin/env bash
# Runs one TSseek query on the cluster: index lookup, then candidate retrieval and exact refinement,
# in one Spark job (TSseek_04_QueryProc_Subseq handles both matching modes).
#
# Usage:
#   ./run_query.sh WHOLE_SEQUENCE '<query>'
#   ./run_query.sh SUBSEQUENCE <SUBSEQUENCE_CASE> '<query>'
# Set the dataset's TSSEEK_* variables first (README, "Run settings"). DRY_RUN=1 prints the command only.
set -euo pipefail

JAR=${JAR:-target/regext_bigdata_research_shark-1.0-SNAPSHOT-jar-with-dependencies.jar}
SPARK_SUBMIT=${SPARK_SUBMIT:-spark-submit}
SPARK_MASTER=${SPARK_MASTER:-spark://YOUR_SPARK_MASTER_HOST:7077}
CORES_MAX=${CORES_MAX:-88}

usage() { sed -n '5,8p' "$0" >&2; exit 1; }
[ $# -ge 2 ] || usage
case $1 in
  WHOLE_SEQUENCE)
    [ $# -eq 2 ] || usage
    unset TSSEEK_SUBSEQUENCE_CASE TSSEEK_WHOLE_CASE
    export TSSEEK_MATCHING_TYPE=WHOLE_SEQUENCE TSSEEK_QUERY_PATTERN=$2 ;;
  SUBSEQUENCE)
    [ $# -eq 3 ] || usage
    unset TSSEEK_WHOLE_CASE
    export TSSEEK_MATCHING_TYPE=SUBSEQUENCE TSSEEK_SUBSEQUENCE_CASE=$2 TSSEEK_QUERY_PATTERN=$3 ;;
  *) usage ;;
esac
: "${TSSEEK_DATASET:?set the dataset TSSEEK_* variables first, see README Run settings}"

cmd=("$SPARK_SUBMIT" --master "$SPARK_MASTER" --deploy-mode client
     --class org.example.bigdata.TSseek_04_QueryProc_Subseq
     --driver-memory 64g --conf spark.cores.max="$CORES_MAX" "$JAR")

echo "TSSEEK settings:"
env | grep '^TSSEEK_' | sort
if [ "${DRY_RUN:-0}" = 1 ]; then
  printf '%q ' "${cmd[@]}"; echo
else
  exec "${cmd[@]}"
fi
