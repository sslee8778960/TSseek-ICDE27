package org.example.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.util.SerializableConfiguration
import java.sql.{Connection, DriverManager}
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedReader, InputStreamReader}
import scala.util.control.NonFatal

object TSseek_05_RetrievalRefinement {
  @transient private lazy val logger = LoggerFactory.getLogger(TSseek_05_RetrievalRefinement.getClass)
  val DATASET = TSseek_Config.DATASET
  val DATASET_SIZE = TSseek_Config.DATASET_SIZE
  val ALPHA = TSseek_Config.ALPHA
  val MATCHING_TYPE = TSseek_Config.MATCHING_TYPE

  val datasetParams = TSseek_Config.getParams(DATASET, ALPHA)

  val QUERY_PATTERN = TSseek_Config.getPattern(DATASET, MATCHING_TYPE)

  val GROUND_TRUTH_SEEDS: Set[Long] = TSseek_Config.getGroundTruthSeeds(DATASET)
  val DB_NAME = "postgres"
  val DB_USER = "postgres"
  val DB_PASSWORD = "YOUR_PASSWORD_HERE"
  val TABLE_PREFIX = TSseek_Config.rawTablePrefix
  val NUM_TABLES = TSseek_Config.numTables(DATASET_SIZE)

  val tableToMachine: Map[Int, String] =
    (1 to NUM_TABLES/2).map(_ -> "shark1").toMap ++
    ((NUM_TABLES/2 + 1) to NUM_TABLES).map(_ -> "shark2").toMap
  val BATCH_SIZE = 10000

  sealed trait Module extends Serializable {
    def minLength: Int
    def maxLength: Int

    def matchesAt(values: Array[Double], start: Int, len: Int): Boolean
    def prefixClosed: Boolean
    def text: String
    def matches(values: Array[Double]): Boolean =
      values.length >= minLength && values.length <= maxLength && matchesAt(values, 0, values.length)
  }

  case class ValueModule(domain: TSseek_03_QueryLanguage.Domain, text: String) extends Module {
    def minLength: Int = 1
    def maxLength: Int = 1
    def prefixClosed: Boolean = true
    def matchesAt(values: Array[Double], start: Int, len: Int): Boolean = len == 1 && domain.contains(values(start))
  }
  case class TrendModule(domain: TSseek_03_QueryLanguage.Domain, direction: String,
                         magnitude: TSseek_03_QueryLanguage.Magnitude, tolerance: Double,
                         minLength: Int, maxLength: Int, text: String) extends Module {
    def prefixClosed: Boolean = true
    def matchesAt(values: Array[Double], start: Int, len: Int): Boolean = {
      if (len < minLength || len > maxLength) return false
      val end = start + len
      var i = start
      while (i < end) {
        if (!domain.contains(values(i))) return false
        i += 1
      }
      i = start + 1
      while (i < end) {
        if (!TSseek_03_QueryLanguage.stepSatisfies(direction, magnitude, tolerance, values(i - 1), values(i))) return false
        i += 1
      }
      true
    }
  }
  case class GapModule(minLength: Int, maxLength: Int, text: String) extends Module {
    def prefixClosed: Boolean = false
    def matchesAt(values: Array[Double], start: Int, len: Int): Boolean = len >= minLength && len <= maxLength
  }
  def main(args: Array[String]): Unit = {
    val overallStartTime = System.nanoTime()
    val conf = new SparkConf()
      .setAppName(s"UnifiedRetrievalRefinement_${DATASET}_${DATASET_SIZE}")
      .set("spark.executor.memory", "10g")
      .set("spark.executor.cores", "4")
      .set("spark.network.timeout", "800s")
      .set("spark.executor.heartbeatInterval", "60s")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    try {
      println("\n" + "=" * 80)
      println(s"UNIFIED RETRIEVAL + REFINEMENT PIPELINE")
      println(s"Dataset: $DATASET $DATASET_SIZE")
      println(s"Alpha: $ALPHA")
      println(s"Matching Type: $MATCHING_TYPE")
      println("=" * 80)

      val alphaStr = f"$ALPHA%.2f"
      val queryResultsPath = DATASET match {
        case "TSBS" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs/query_results_alpha_${alphaStr}"
        case "ECG" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg/query_results_alpha_${alphaStr}"
        case "RANDOMWALK" => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk/query_results_alpha_${alphaStr}"
        case _ => throw new IllegalArgumentException(s"Unknown dataset: $DATASET")
      }
      println(s"\nReading query results from: $queryResultsPath")
      println(s"Processing $NUM_TABLES tables (based on dataset size: $DATASET_SIZE)")
      val segPrefix = s"${TSseek_Config.datasetPrefix()}_segments"
      val loadStart = System.nanoTime()
      val perTableIds: Map[String, Set[Int]] = spark.sparkContext
        .wholeTextFiles(s"$queryResultsPath/query_results_table*.csv", NUM_TABLES)
        .collect()
        .flatMap { case (path, content) =>
          val tableNum = """query_results_table(\d+)\.csv""".r
            .findFirstMatchIn(path).map(_.group(1).toInt).getOrElse(-1)
          if (tableNum < 0) None
          else {
            val ids = content.split('\n').iterator
              .map(_.trim)
              .filter(l => l.nonEmpty && !l.startsWith("time_series_id"))
              .flatMap(l => scala.util.Try(l.toLong.toInt).toOption)
              .toSet
            Some(s"${segPrefix}_table$tableNum" -> ids)
          }
        }.toMap
      val loadElapsed = (System.nanoTime() - loadStart) / 1e9d
      println(f"Loaded candidate IDs for ${perTableIds.size}%d tables in $loadElapsed%.2f s " +
        f"(total ${perTableIds.values.map(_.size).sum}%,d candidates)")
      if (perTableIds.size < NUM_TABLES) {
        val present = perTableIds.keys
          .flatMap(k => """table(\d+)$""".r.findFirstMatchIn(k).map(_.group(1).toInt)).toSet
        val missing = (1 to NUM_TABLES).filterNot(present.contains)
        println(s"WARNING: candidate files missing for ${missing.size} tables: ${missing.mkString(", ")}")
        println("         (legitimate only if Stage 1 wrote no file for those tables)")
      }
      val outputBase = DATASET match {
        case "TSBS"       => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs"
        case "ECG"        => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg"
        case "RANDOMWALK" => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk"
        case _            => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/${DATASET.toLowerCase}"
      }
      if (MATCHING_TYPE == "SUBSEQUENCE") {
        runSubsequenceRefinementInline(spark, perTableIds, QUERY_PATTERN, outputBase, GROUND_TRUTH_SEEDS)
      } else if (MATCHING_TYPE == "WHOLE_SEQUENCE") {
        runWholeRefinementInline(spark, perTableIds, QUERY_PATTERN, outputBase, GROUND_TRUTH_SEEDS)
      } else {
        throw new IllegalArgumentException(s"Unknown MATCHING_TYPE: $MATCHING_TYPE")
      }
      val overallEndTime = System.nanoTime()
      val totalDuration = (overallEndTime - overallStartTime) / 1e9d / 60.0
      println("\n" + "=" * 80)
      println(s"✓ PIPELINE COMPLETE!")
      println(f"Total time: $totalDuration%.2f minutes")
      println("=" * 80)
    } finally {
      spark.stop()
    }
  }
  def loadCandidateIds(fs: FileSystem, queryFilePath: String): Array[Long] = {
    val path = new Path(queryFilePath)
    if (!fs.exists(path)) {
      println(s"[Candidates] File not found: $queryFilePath")
      return Array.emptyLongArray
    }
    val buffer = ArrayBuffer[Long]()
    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(new InputStreamReader(fs.open(path)))
      var line = reader.readLine()
      while (line != null) {
        val trimmed = line.trim
        if (trimmed.nonEmpty && trimmed != "time_series_id" && !trimmed.startsWith("time_series_id")) {
          try {
            buffer += trimmed.toLong
          } catch {
            case NonFatal(e) =>
              println(s"[Candidates] Skipping invalid value '$trimmed' in $queryFilePath: ${e.getMessage}")
          }
        }
        line = reader.readLine()
      }
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
    buffer.distinct.toArray
  }
  def processTableSubsequenceWithIds(
    tableNum: Int,
    chunkIdx: Int,
    candidateIds: Array[Long],
    machine: String,
    pgTableName: String,
    multiPattern: TSseek_05_SubseqDFA.MultiPattern,
    existenceOnly: Boolean
  ): (Int, Long, Long, Double, Double, Array[TSseek_05_SubseqDFA.MultiPatternMatch]) = {
    val startTime = System.nanoTime()

    println(s"\n[Table $tableNum chunk $chunkIdx on $machine] subseq refinement — ${candidateIds.length} candidate IDs")
    if (candidateIds.isEmpty) {
      return (tableNum, 0L, 0L, 0.0, 0.0, Array.empty[TSseek_05_SubseqDFA.MultiPatternMatch])
    }
    try {
      val (matches, retrievalTime, refinementTime) =
        retrieveAndRefineSubsequenceMulti(machine, pgTableName, candidateIds, multiPattern, existenceOnly)
      val totalTime = (System.nanoTime() - startTime) / 1e9d
      println(f"[Table $tableNum%d chunk $chunkIdx%d] done in $totalTime%.2f s (${matches.length} matching series)")
      (tableNum, candidateIds.length.toLong, matches.length.toLong, retrievalTime, refinementTime, matches)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"[Table $tableNum chunk $chunkIdx on $machine] refinement FAILED: ${e.getMessage}", e)
    }
  }

  private def chunkCandidateTasks(
    candidateIdsByTable: Map[String, Set[Int]],
    totalCores: Int
  ): (Array[(Int, Int, Array[Long], String, String)], Int) = {
    val totalCandidates = candidateIdsByTable.values.map(_.size).sum
    val targetTasks = math.max(1, totalCores * TSseek_Config.TASKS_PER_CORE)
    val chunkSize = math.max(TSseek_Config.MIN_CHUNK,
      math.ceil(totalCandidates.toDouble / targetTasks).toInt)

    val chunks = candidateIdsByTable.toArray.flatMap { case (segName, ids) =>
      val tableNum = """table(\d+)$""".r.findFirstMatchIn(segName).map(_.group(1).toInt).getOrElse(-1)
      val pgTableName = segName.replace("_segments_", "_og_ts_")
      val machine = if (tableNum > 0 && tableNum <= NUM_TABLES / 2) "shark1" else "shark2"
      val sorted = ids.toArray.map(_.toLong).sorted
      if (sorted.isEmpty) Array.empty[(Int, Int, Array[Long], String, String)]
      else sorted.grouped(chunkSize).zipWithIndex.map { case (c, ci) =>
        (tableNum, ci, c, machine, pgTableName)
      }.toArray
    }.sortBy(t => (t._1, t._2))
    (chunks, chunkSize)
  }
  private def reportChunkTimings[T](results: Array[(Int, Long, Long, Double, Double, T)]): Unit = {
    results.zipWithIndex.foreach { case ((table, candidates, matches, retrieval, refinement, _), index) =>
      println(f"CHUNK_TIMING result_index=$index%d table=$table%d candidates=$candidates%d matches=$matches%d retrieval_seconds=$retrieval%.6f refinement_seconds=$refinement%.6f")
    }
    val retrieval = results.map(_._4)
    val refinement = results.map(_._5)
    println(f"TASK_TIMING chunks=${results.length}%d retrieval_sum_seconds=${retrieval.sum}%.6f refinement_sum_seconds=${refinement.sum}%.6f retrieval_min_seconds=${if (retrieval.isEmpty) 0.0 else retrieval.min}%.6f retrieval_max_seconds=${if (retrieval.isEmpty) 0.0 else retrieval.max}%.6f refinement_min_seconds=${if (refinement.isEmpty) 0.0 else refinement.min}%.6f refinement_max_seconds=${if (refinement.isEmpty) 0.0 else refinement.max}%.6f")
  }
  private def reportCandidateSeeds(candidateIdsByTable: Map[String, Set[Int]], groundTruthSeeds: Set[Long]): Unit = {
    val candidateSeeds = groundTruthSeeds.filter(id => candidateIdsByTable.valuesIterator.exists(_.contains(id.toInt)))
    println(s"SEED_STAGE candidate_positive_ids=${candidateSeeds.toSeq.sorted.mkString(",")} candidate_positive_count=${candidateSeeds.size} expected_positive_count=${groundTruthSeeds.size}")
  }
  def runSubsequenceRefinementInline(
    spark: SparkSession,
    candidateIdsByTable: Map[String, Set[Int]],
    queryPattern: String,
    saveOutputBaseHdfs: String,
    groundTruthSeeds: Set[Long]
  ): (Long, Long, Set[Long]) = {
    val sc = spark.sparkContext
    val stage2Start = System.nanoTime()
    val existenceOnly = !TSseek_Config.OUTPUT_POSITIONS
    val outputPositions = TSseek_Config.OUTPUT_POSITIONS

    println("\n" + "=" * 80)
    println("FUSED STAGE 2: Parallel Retrieval + DFA Refinement (in-memory IDs from Stage 1)")
    println("=" * 80)
    println(s"Tables to process: ${candidateIdsByTable.size}")
    println(s"Total candidate IDs: ${candidateIdsByTable.values.map(_.size).sum}")
    reportCandidateSeeds(candidateIdsByTable, groundTruthSeeds)
    println(s"Existence-only matching: $existenceOnly (output positions: $outputPositions)")
    val multiPattern = TSseek_05_SubseqDFA.parseAllPatterns(queryPattern)
    println(s"Patterns: ${multiPattern.patterns.length}, Separator type: ${multiPattern.separatorType}")
    val multiPatternBroadcast = sc.broadcast(multiPattern)
    val totalCores = sc.defaultParallelism
    val (chunks, chunkSize) = chunkCandidateTasks(candidateIdsByTable, totalCores)
    println(s"Task decomposition: ${chunks.length} chunks (chunkSize=$chunkSize, cores=$totalCores, " +
      s"target=${totalCores * TSseek_Config.TASKS_PER_CORE})")
    val mapPartStart = System.nanoTime()
    val results =
      if (chunks.isEmpty) Array.empty[(Int, Long, Long, Double, Double, Array[TSseek_05_SubseqDFA.MultiPatternMatch])]
      else sc
        .parallelize(chunks.toSeq, chunks.length)
        .mapPartitions { partitionIter =>
          val localMultiPattern = multiPatternBroadcast.value
          partitionIter.map { case (tableNum, chunkIdx, candidateIds, machine, pgTableName) =>
            processTableSubsequenceWithIds(tableNum, chunkIdx, candidateIds, machine, pgTableName,
              localMultiPattern, existenceOnly)
          }.toList.iterator
        }
        .collect()
        .sortBy(_._1)
    val mapPartElapsed = (System.nanoTime() - mapPartStart) / 1e9d
    println(f"PHASE_TIMING stage2_mappartitions=$mapPartElapsed%.2f")
    reportChunkTimings(results)
    val totalCandidates = results.map(_._2).sum
    val totalMatches = results.map(_._3).sum
    val avgRetrieval = if (results.nonEmpty) results.map(_._4).sum / results.length else 0.0
    val avgRefinement = if (results.nonEmpty) results.map(_._5).sum / results.length else 0.0
    val allMatches = results.flatMap(_._6)
    println("\n" + "=" * 80)
    println("FUSED STAGE 2: Aggregate Results")
    println("=" * 80)
    println(f"Total candidates retrieved: $totalCandidates%,d")
    println(f"Total matching series: ${allMatches.map(_.timeSeriesId).distinct.length}%,d")
    println(f"Average retrieval time per chunk: $avgRetrieval%.2f seconds")
    println(f"Average refinement time per chunk: $avgRefinement%.2f seconds")
    val matchRate = if (totalCandidates == 0) 0.0 else totalMatches.toDouble / totalCandidates * 100.0
    println(f"Match rate: $matchRate%.2f%%")
    val matchingTimeSeriesIds: Set[Long] = allMatches.map(_.timeSeriesId).toSet
    val foundSeeds: Set[Long] = matchingTimeSeriesIds.intersect(groundTruthSeeds)
    if (groundTruthSeeds.nonEmpty) {
      val accuracy = (foundSeeds.size.toDouble / groundTruthSeeds.size) * 100.0
      println("\n" + "=" * 80)
      println("ACCURACY REPORT")
      println("=" * 80)
      println(f"Ground truth seeds: ${groundTruthSeeds.size}%d")
      println(f"Seeds found: ${foundSeeds.size}%d")
      println(f"Accuracy: $accuracy%.2f%% (${foundSeeds.size}/${groundTruthSeeds.size})")
      if (foundSeeds.nonEmpty)
        println(s"Found seed IDs: ${foundSeeds.toSeq.sorted.mkString(", ")}")
    }
    val saveStart = System.nanoTime()
    val outputPath = subsequenceOutputPath(saveOutputBaseHdfs)
    if (outputPositions && allMatches.nonEmpty) {
      println(s"\nSaving ${allMatches.length} matches (positions) to: $outputPath")
      val flatMatches = allMatches.zipWithIndex.flatMap { case (mpm, matchIdx) =>
        mpm.patternMatches.zipWithIndex.map { case (m, idx) =>
          (mpm.timeSeriesId, matchIdx, idx, m.startPosition, m.endPosition, m.length)
        }
      }
      val matchesRDD = sc.parallelize(flatMatches.sortBy(t => (t._1, t._2, t._3)))
        .map { case (tsId, matchIdx, patIdx, start, end, len) => s"$tsId,$matchIdx,$patIdx,$start,$end,$len" }
        .coalesce(1)
      matchesRDD.saveAsTextFile(outputPath)
      println(s"✓ Results saved: $outputPath")
    } else {
      saveIdResult(sc, outputPath, matchingTimeSeriesIds.toSeq.sorted)
    }
    val saveElapsed = (System.nanoTime() - saveStart) / 1e9d
    println(f"PHASE_TIMING stage2_save=$saveElapsed%.2f")

    multiPatternBroadcast.destroy()
    val stage2Total = (System.nanoTime() - stage2Start) / 1e9d
    println(f"PHASE_TIMING stage2_total=$stage2Total%.2f")
    (totalCandidates, matchingTimeSeriesIds.size.toLong, foundSeeds)
  }

  def runWholeRefinementInline(
    spark: SparkSession,
    candidateIdsByTable: Map[String, Set[Int]],
    queryPattern: String,
    saveOutputBaseHdfs: String,
    groundTruthSeeds: Set[Long]
  ): (Long, Long, Set[Long]) = {
    val sc = spark.sparkContext
    val stage2Start = System.nanoTime()
    println("\n" + "=" * 80)
    println("FUSED STAGE 2 (WHOLE): Parallel Retrieval + DFA Refinement")
    println("=" * 80)
    println(s"Tables to process: ${candidateIdsByTable.size}")
    println(s"Total candidate IDs: ${candidateIdsByTable.values.map(_.size).sum}")
    reportCandidateSeeds(candidateIdsByTable, groundTruthSeeds)
    val modules = buildWholeModulesFrom(queryPattern)
    val maxTotal = if (modules.exists(_.maxLength == Int.MaxValue)) "unbounded" else modules.map(_.maxLength.toLong).sum.toString
    println(s"Whole-matching modules: ${modules.length} (minLen=${modules.map(_.minLength.toLong).sum}, maxLen=$maxTotal); " +
      "original predicates, no epsilon; implicit trailing wildcard only when maxLen < series length")
    val modulesBroadcast = sc.broadcast(modules)
    val totalCores = sc.defaultParallelism
    val (chunks, chunkSize) = chunkCandidateTasks(candidateIdsByTable, totalCores)
    println(s"Task decomposition: ${chunks.length} chunks (chunkSize=$chunkSize, cores=$totalCores, " +
      s"target=${totalCores * TSseek_Config.TASKS_PER_CORE})")
    val mapPartStart = System.nanoTime()
    val results =
      if (chunks.isEmpty) Array.empty[(Int, Long, Long, Double, Double, Array[Long])]
      else sc
        .parallelize(chunks.toSeq, chunks.length)
        .mapPartitions { partitionIter =>
          val localModules = modulesBroadcast.value
          partitionIter.map { case (tableNum, chunkIdx, candidateIds, machine, pgTableName) =>
            val t0 = System.nanoTime()
            println(s"\n[Table $tableNum chunk $chunkIdx on $machine] whole refinement — ${candidateIds.length} candidate IDs")
            try {
              val (matchedIds, tRet, tRef) =
                retrieveAndRefineWholeChunk(machine, pgTableName, candidateIds, localModules)
              val dt = (System.nanoTime() - t0) / 1e9d
              println(f"[Table $tableNum%d chunk $chunkIdx%d] done in $dt%.2f s (${matchedIds.length} matches)")
              (tableNum, candidateIds.length.toLong, matchedIds.length.toLong, tRet, tRef, matchedIds)
            } catch {
              case e: Exception =>
                throw new RuntimeException(s"[Table $tableNum chunk $chunkIdx on $machine] whole refinement FAILED: ${e.getMessage}", e)
            }
          }.toList.iterator
        }
        .collect()
        .sortBy(_._1)
    val mapPartElapsed = (System.nanoTime() - mapPartStart) / 1e9d
    println(f"PHASE_TIMING stage2_mappartitions=$mapPartElapsed%.2f")
    reportChunkTimings(results)
    val totalCandidates = results.map(_._2).sum
    val matchingIds: Set[Long] = results.flatMap(_._6).toSet
    val avgRetrieval = if (results.nonEmpty) results.map(_._4).sum / results.length else 0.0
    val avgRefinement = if (results.nonEmpty) results.map(_._5).sum / results.length else 0.0
    println("\n" + "=" * 80)
    println("FUSED STAGE 2 (WHOLE): Aggregate Results")
    println("=" * 80)
    println(f"Total candidates retrieved: $totalCandidates%,d")
    println(f"Total matching series: ${matchingIds.size}%,d")
    println(f"Average retrieval time per chunk: $avgRetrieval%.2f seconds")
    println(f"Average refinement time per chunk: $avgRefinement%.2f seconds")
    val matchRate = if (totalCandidates == 0) 0.0 else matchingIds.size.toDouble / totalCandidates * 100.0
    println(f"Match rate: $matchRate%.2f%%")
    val foundSeeds: Set[Long] = matchingIds.intersect(groundTruthSeeds)
    if (groundTruthSeeds.nonEmpty) {
      val accuracy = (foundSeeds.size.toDouble / groundTruthSeeds.size) * 100.0
      println("\n" + "=" * 80)
      println("ACCURACY REPORT")
      println("=" * 80)
      println(f"Ground truth seeds: ${groundTruthSeeds.size}%d")
      println(f"Seeds found: ${foundSeeds.size}%d")
      println(f"Accuracy: $accuracy%.2f%% (${foundSeeds.size}/${groundTruthSeeds.size})")
      if (foundSeeds.nonEmpty)
        println(s"Found seed IDs: ${foundSeeds.toSeq.sorted.mkString(", ")}")
      if (foundSeeds.size < groundTruthSeeds.size)
        println(s"Missed seed IDs: ${(groundTruthSeeds -- foundSeeds).toSeq.sorted.mkString(", ")}")
    }
    val saveStart = System.nanoTime()
    saveIdResult(sc, wholeOutputPath(saveOutputBaseHdfs), matchingIds.toSeq.sorted)
    val saveElapsed = (System.nanoTime() - saveStart) / 1e9d
    println(f"PHASE_TIMING stage2_save=$saveElapsed%.2f")
    modulesBroadcast.destroy()
    val stage2Total = (System.nanoTime() - stage2Start) / 1e9d
    println(f"PHASE_TIMING stage2_total=$stage2Total%.2f")
    (totalCandidates, matchingIds.size.toLong, foundSeeds)
  }
  def wholeOutputPath(saveOutputBaseHdfs: String): String = {
    val timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    s"$saveOutputBaseHdfs/dfa_matches_alpha_${f"$ALPHA%.2f"}_$timestamp"
  }
  def subsequenceOutputPath(saveOutputBaseHdfs: String): String = {
    val timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    s"$saveOutputBaseHdfs/subsequence_matches_$timestamp"
  }
  def saveIdResult(sc: org.apache.spark.SparkContext, outputPath: String, ids: Seq[Long]): Unit = {
    println(s"\nSaving ${ids.size} matching ts_ids to: $outputPath")
    sc.parallelize(ids.map(_.toString), 1).saveAsTextFile(outputPath)
    println(s"✓ Results saved: $outputPath")
  }
  def writeShortCircuitResult(spark: SparkSession, outputPath: String, reason: String): Unit = {
    println(s"ZERO_COUNT_SHORT_CIRCUIT reason=$reason; Stage 1 SQL and Stage 2 skipped")
    println("PHASE_TIMING stage2_wall=0.00")
    saveIdResult(spark.sparkContext, outputPath, Seq.empty)
  }
  def retrieveAndRefineWholeChunk(
    machine: String,
    tableName: String,
    candidateIds: Array[Long],
    modules: Array[Module]
  ): (Array[Long], Double, Double) = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val matched = ArrayBuffer[Long]()
    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null
    var totalRetrievalNanos = 0L
    var totalRefinementNanos = 0L
    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")
      candidateIds.grouped(BATCH_SIZE).foreach { batch =>
        val startRetrieve = System.nanoTime()
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()
        val retrievedBatch = ArrayBuffer[(Long, String)]()
        while (rs.next()) {
          retrievedBatch += rs.getLong("ts_id") -> rs.getString("ts_data")
        }
        rs.close()
        sqlArray.free()
        totalRetrievalNanos += System.nanoTime() - startRetrieve
        val startRefine = System.nanoTime()
        retrievedBatch.foreach { case (tsId, tsData) =>
          val values = TSseek_05_SubseqDFA.parseTimeSeriesFast(tsData)
          if (matchWholeValues(values, modules)) matched += tsId
        }
        totalRefinementNanos += System.nanoTime() - startRefine
      }
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    (matched.toArray, totalRetrievalNanos / 1e9d, totalRefinementNanos / 1e9d)
  }
  def matchWholeValues(values: Array[Double], modules: Array[Module]): Boolean = {
    val n = values.length
    if (n == 0) return false
    val maxTotal = modules.foldLeft(0L)((acc, m) => if (acc == Long.MaxValue || m.maxLength == Int.MaxValue) Long.MaxValue else acc + m.maxLength)
    val mods = if (maxTotal < n) modules :+ GapModule(0, Int.MaxValue, "<implicit trailing wildcard>") else modules
    val suffixMin = new Array[Long](mods.length + 1)
    var i = mods.length - 1
    while (i >= 0) { suffixMin(i) = suffixMin(i + 1) + mods(i).minLength; i -= 1 }
    if (suffixMin(0) > n) return false
    val failed = if (mods.exists(m => m.minLength != m.maxLength)) new java.util.BitSet((mods.length + 1) * (n + 1)) else null
    matchFrom(values, mods, suffixMin, 0, 0, failed)
  }
  private def matchFrom(values: Array[Double], modules: Array[Module], suffixMin: Array[Long],
                        valueIdx: Int, moduleIdx: Int, failed: java.util.BitSet): Boolean = {
    val n = values.length
    if (moduleIdx == modules.length) return valueIdx == n
    if (valueIdx + suffixMin(moduleIdx) > n) return false
    val key = moduleIdx * (n + 1) + valueIdx
    if (failed != null && failed.get(key)) return false
    val module = modules(moduleIdx)
    val maxLen = math.min(module.maxLength.toLong, (n - valueIdx).toLong).toInt
    var len = module.minLength
    while (len <= maxLen) {
      if (module.matchesAt(values, valueIdx, len)) {
        if (matchFrom(values, modules, suffixMin, valueIdx + len, moduleIdx + 1, failed)) return true
      } else if (module.prefixClosed) len = maxLen
      len += 1
    }
    if (failed != null) failed.set(key)
    false
  }

  def processTableSubsequence(
    fs: FileSystem,
    tableNum: Int,
    queryFilePath: String,
    machine: String,
    pgTableName: String,
    multiPattern: TSseek_05_SubseqDFA.MultiPattern
  ): (Int, Long, Long, Double, Double, Array[TSseek_05_SubseqDFA.MultiPatternMatch]) = {
    val startTime = System.nanoTime()
    println(s"\n[Table $tableNum on $machine] Starting subsequence matching processing...")
    try {
      println(s"[Table $tableNum] Reading candidates from HDFS...")
      val candidateIds = loadCandidateIds(fs, queryFilePath)
      println(s"[Table $tableNum] Found ${candidateIds.length} candidates")
      if (candidateIds.isEmpty) {
        println(s"[Table $tableNum] No candidates, skipping...")
        return (tableNum, 0L, 0L, 0.0, 0.0, Array.empty[TSseek_05_SubseqDFA.MultiPatternMatch])
      }
      println(s"[Table $tableNum] Retrieving time series data and applying multi-pattern DFA...")
      val (matches, retrievalTime, refinementTime) =
        retrieveAndRefineSubsequenceMulti(machine, pgTableName, candidateIds, multiPattern)
      val endTime = System.nanoTime()
      val totalTime = (endTime - startTime) / 1e9d
      println(f"[Table $tableNum%d] ✓ Complete in $totalTime%.2f seconds (${matches.length} matches)")
      (tableNum, candidateIds.length.toLong, matches.length.toLong, retrievalTime, refinementTime, matches)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"[Table $tableNum] refinement FAILED: ${e.getMessage}", e)
    }
  }
  def processTable(fs: FileSystem, tableNum: Int, queryFilePath: String, machine: String, pgTableName: String): (Int, Long, Long, Double, Double, Map[Long, String]) = {
    val startTime = System.nanoTime()
    println(s"\n[Table $tableNum on $machine] Starting processing...")
    try {

      println(s"[Table $tableNum] Reading candidates from HDFS...")
      val candidateIds = loadCandidateIds(fs, queryFilePath)
      println(s"[Table $tableNum] Found ${candidateIds.length} candidates")
      if (candidateIds.isEmpty) {
        println(s"[Table $tableNum] No candidates, skipping...")
        return (tableNum, 0L, 0L, 0.0, 0.0, Map.empty[Long, String])
      }
      val retrievalStartTime = System.nanoTime()
      println(s"[Table $tableNum] Retrieving time series data from PostgreSQL...")
      val (matchingData, retrievalTime, refinementTime) =
        retrieveAndRefine(machine, pgTableName, candidateIds)
      val endTime = System.nanoTime()
      val totalTime = (endTime - startTime) / 1e9d
      println(f"[Table $tableNum%d] ✓ Complete in $totalTime%.2f seconds")
      (tableNum, candidateIds.length.toLong, matchingData.size.toLong, retrievalTime, refinementTime, matchingData)
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"[Table $tableNum] refinement FAILED: ${e.getMessage}", e)
    }
  }
  def retrieveAndRefineSubsequence(
    machine: String,
    tableName: String,
    candidateIds: Array[Long],
    pattern: TSseek_05_SubseqDFA.Pattern,
    threshold: TSseek_05_SubseqDFA.Threshold
  ): (Array[TSseek_05_SubseqDFA.Match], Double, Double) = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val matches = ArrayBuffer[TSseek_05_SubseqDFA.Match]()
    val batches = candidateIds.grouped(BATCH_SIZE)
    println(s"  Processing ${candidateIds.length / BATCH_SIZE + 1} batches of size up to $BATCH_SIZE...")
    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null

    var totalRetrievalNanos = 0L
    var totalRefinementNanos = 0L

    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")
      var batchIdx = 0
      batches.foreach { batch =>
        val startRetrieve = System.nanoTime()
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()
        val retrievedBatch = scala.collection.mutable.ArrayBuffer[(Long, String)]()
        while (rs.next()) {
          retrievedBatch += rs.getLong("ts_id") -> rs.getString("ts_data")
        }
        rs.close()
        sqlArray.free()
        totalRetrievalNanos += System.nanoTime() - startRetrieve
        val startRefine = System.nanoTime()
        retrievedBatch.foreach { case (tsId, tsData) =>
          val tsMatches = TSseek_05_SubseqDFA.findMatchesWithStreamingDFA(
            tsId, tsData, pattern, threshold
          )
          matches ++= tsMatches
        }
        totalRefinementNanos += System.nanoTime() - startRefine

        batchIdx += 1
        if (batchIdx % 10 == 0) print(".")
      }
      println()
    } catch {
      case e: Exception =>
        if (conn != null) conn.close()
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    (matches.toArray, totalRetrievalNanos / 1e9d, totalRefinementNanos / 1e9d)
  }
  def retrieveAndRefineSubsequenceMulti(
    machine: String,
    tableName: String,
    candidateIds: Array[Long],
    multiPattern: TSseek_05_SubseqDFA.MultiPattern,
    existenceOnly: Boolean = false
  ): (Array[TSseek_05_SubseqDFA.MultiPatternMatch], Double, Double) = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val matches = ArrayBuffer[TSseek_05_SubseqDFA.MultiPatternMatch]()
    val batches = candidateIds.grouped(BATCH_SIZE)
    println(s"  Processing ${candidateIds.length / BATCH_SIZE + 1} batches of size up to $BATCH_SIZE...")
    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null
    var totalRetrievalNanos = 0L
    var totalRefinementNanos = 0L
    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")
      var batchIdx = 0
      batches.foreach { batch =>
        val startRetrieve = System.nanoTime()
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()
        val retrievedBatch = scala.collection.mutable.ArrayBuffer[(Long, String)]()
        while (rs.next()) {
          retrievedBatch += rs.getLong("ts_id") -> rs.getString("ts_data")
        }
        rs.close()
        sqlArray.free()
        totalRetrievalNanos += System.nanoTime() - startRetrieve
        val startRefine = System.nanoTime()
        retrievedBatch.foreach { case (tsId, tsData) =>
          val tsMatches = TSseek_05_SubseqDFA.findMultiPatternMatches(
            tsId, tsData, multiPattern, existenceOnly
          )
          matches ++= tsMatches
        }
        totalRefinementNanos += System.nanoTime() - startRefine
        batchIdx += 1
        if (batchIdx % 10 == 0) print(".")
      }
      println()

    } catch {
      case e: Exception =>
        if (conn != null) conn.close()
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    (matches.toArray, totalRetrievalNanos / 1e9d, totalRefinementNanos / 1e9d)
  }
  def retrieveAndRefine(machine: String,
                        tableName: String,
                        candidateIds: Array[Long]): (Map[Long, String], Double, Double) = {
    val url = s"jdbc:postgresql://$machine:5432/$DB_NAME"
    val matching = scala.collection.mutable.Map[Long, String]()
    val batches = candidateIds.grouped(BATCH_SIZE)
    println(s"  Processing ${candidateIds.length / BATCH_SIZE + 1} batches of size up to $BATCH_SIZE...")
    var conn: Connection = null
    var stmt: java.sql.PreparedStatement = null
    var totalRetrievalNanos = 0L
    var totalRefinementNanos = 0L
    try {
      Class.forName("org.postgresql.Driver")
      conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD)
      stmt = conn.prepareStatement(s"SELECT ts_id, ts_data FROM $tableName WHERE ts_id = ANY (?)")
      var batchIdx = 0
      batches.foreach { batch =>
        val startRetrieve = System.nanoTime()
        val sqlArray = conn.createArrayOf("BIGINT", batch.map(Long.box))
        stmt.setArray(1, sqlArray)
        val rs = stmt.executeQuery()
        val retrievedBatch = scala.collection.mutable.ArrayBuffer[(Long, String)]()
        while (rs.next()) {
          retrievedBatch += rs.getLong("ts_id") -> rs.getString("ts_data")
        }
        rs.close()
        sqlArray.free()
        totalRetrievalNanos += System.nanoTime() - startRetrieve
        val startRefine = System.nanoTime()
        retrievedBatch.foreach { case (tsId, tsData) =>
          if (matchPattern(tsData)) {
            matching += tsId -> tsData
          }
        }
        totalRefinementNanos += System.nanoTime() - startRefine
        batchIdx += 1
        if (batchIdx % 10 == 0) print(".")
      }
      println()
    } catch {
      case e: Exception =>
        if (conn != null) conn.close()
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    (matching.toMap, totalRetrievalNanos / 1e9d, totalRefinementNanos / 1e9d)
  }
  def parseTimeSeries(tsString: String): Array[Double] = {
    try {
      val pattern = """MULTIPOINT\(\((.*)\)\)""".r
      tsString match {
        case pattern(points) =>
          points.split("\\), \\(").map { point =>
            val parts = point.trim.split("\\s+")
            if (parts.length >= 2) parts(1).toDouble
            else throw new Exception(s"Invalid point format: $point")
          }
        case _ =>
          logger.warn(s"Invalid time series format: ${tsString.take(50)}...")
          Array.empty[Double]
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error parsing time series: ${e.getMessage}")
        Array.empty[Double]
    }
  }

  def createPatternModules(): Array[Module] = buildWholeModulesFrom(QUERY_PATTERN)

  def buildWholeModulesFrom(pattern: String): Array[Module] = {
    val query = TSseek_03_QueryWindowParser.parseWholeQuery(pattern)
    val tolerance = query.threshold.value
    query.constructs.map {
      case c: TSseek_03_QueryWindowParser.SingletonConstruct =>
        ValueModule(TSseek_03_QueryLanguage.Domain.point(c.value), c.text): Module
      case c: TSseek_03_QueryWindowParser.RangeConstruct => ValueModule(c.domain, c.text)
      case c: TSseek_03_QueryWindowParser.PatternConstruct =>
        TrendModule(c.domain, c.direction, c.magnitude, tolerance, c.minLen, c.maxLen, c.text)
      case c: TSseek_03_QueryWindowParser.WildcardConstruct => GapModule(c.minLen, c.maxLen, c.text)
    }.toArray
  }

  def matchPattern(tsString: String): Boolean = {
    val values = parseTimeSeries(tsString)
    if (values.isEmpty) return false
    matchWholeValues(values, createPatternModules())
  }

  def performDFARefinement(timeSeriesData: Map[Long, String]): Set[Long] = {
    timeSeriesData.iterator.collect { case (tsId, tsData) if matchPattern(tsData) => tsId }.toSet
  }
}
