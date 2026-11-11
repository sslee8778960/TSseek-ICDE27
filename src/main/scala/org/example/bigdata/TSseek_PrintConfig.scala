package org.example.bigdata

object TSseek_PrintConfig {
  def main(args: Array[String]): Unit = {
    println(s"DATASET=${TSseek_Config.DATASET}")
    println(s"DATASET_SIZE=${TSseek_Config.DATASET_SIZE}")
    println(s"ALPHA=${TSseek_Config.ALPHA}")
    println(s"MATCHING_TYPE=${TSseek_Config.MATCHING_TYPE}")
    println(s"SUBSEQUENCE_CASE=${TSseek_Config.SUBSEQUENCE_CASE}")
    println(s"MULTI_PATTERN_STRATEGY=${TSseek_Config.MULTI_PATTERN_STRATEGY}")
    println(s"USE_FLUCTUATION_FILTER=${TSseek_Config.USE_FLUCTUATION_FILTER}")
    println(s"LEGACY_FLUCTUATION_FILTER=${TSseek_Config.LEGACY_FLUCTUATION_FILTER}")
    println(s"SLOPE_FILTER=${TSseek_Config.SLOPE_FILTER}")
    println(s"STEP_FILTER=${TSseek_Config.STEP_FILTER}")
    println(s"REWRITE_RULES=${TSseek_Config.REWRITE_RULES}")
    println(s"SERIES_LENGTH=${TSseek_Config.SERIES_LENGTH}")
    println(s"DATASET_PREFIX=${TSseek_Config.datasetPrefix()}")
    println(s"WHOLE_CASE=${TSseek_Config.WHOLE_CASE}")
  }
}
