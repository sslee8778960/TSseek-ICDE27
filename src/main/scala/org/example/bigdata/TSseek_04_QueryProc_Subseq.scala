package org.example.bigdata

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import java.io._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TSseek_04_QueryProc_Subseq {
  private val logger = LoggerFactory.getLogger(getClass)
  def readMapFromHDFS[T, U](filePath: String,
                            spark: SparkSession,
                            keyCast: String => T,
                            valueCast: String => U): Map[T, U] = {
    val rdd = spark.sparkContext.textFile(filePath)
    val pairs = rdd.collect().flatMap { line =>
      line.split(",").toList match {
        case k :: v :: Nil => Some(keyCast(k) -> valueCast(v))
        case _             => None
      }
    }
    pairs.toMap
  }

  private def wholeProbeSql(table: String, c: TSseek_03_QueryWindowParser.ConstructProbes): String = {
    val filters = c.filters.map(_.sql).mkString
    c.windows.map { w =>
      s"""SELECT DISTINCT time_series_id FROM $table
         |WHERE ST_Intersects(time_series_segments, ${TSseek_03_IndexContract.probeGeometrySql(w)})$filters""".stripMargin
    }.mkString("\nINTERSECT\n")
  }

  def executeDesignSpaceSql(
    tableIdentifier: String,
    hostMachine: String,
    databases: Map[String, String],
    sql: String,
    threshold: Double,
    span90: Double
  ): Future[Set[Int]] = Future {
    val url = databases(hostMachine)
    val props = new java.util.Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "YOUR_PASSWORD_HERE")
    var conn: java.sql.Connection = null
    var stmt: java.sql.Statement = null
    var rs: java.sql.ResultSet = null
    val ids = mutable.Set[Int]()
    val t0 = System.currentTimeMillis()
    val useStreaming = threshold >= span90
    try {
      conn = DriverManager.getConnection(url, props)
      if (useStreaming) {
        stmt = conn.createStatement(
          java.sql.ResultSet.TYPE_FORWARD_ONLY,
          java.sql.ResultSet.CONCUR_READ_ONLY
        )
        stmt.setFetchSize(20000)
        logger.info(s"[$hostMachine.$tableIdentifier] Using streaming mode for design space query")
      } else {
        stmt = conn.createStatement()
      }
      stmt.execute("SET max_parallel_workers_per_gather = 0")
      logger.info(s"[$hostMachine.$tableIdentifier] Executing design space SQL")
      logger.debug(s"SQL: $sql")
      rs = stmt.executeQuery(sql)
      while (rs.next()) ids += rs.getInt("time_series_id")
      val t1 = System.currentTimeMillis()
      logger.info(s"[$hostMachine.$tableIdentifier] Design space query retrieved ${ids.size} IDs in ${(t1 - t0) / 1000.0}s")
    } finally {
      if (rs != null) rs.close()
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
    ids.toSet
  }

  def saveIdsToCsv(tableIdentifier: String, ids: Set[Int], runIdx: Int, timestamp: String): Unit = {
    val matchingType = TSseek_Config.MATCHING_TYPE.toLowerCase
    val subCase = if (TSseek_Config.MATCHING_TYPE == "SUBSEQUENCE") s"_${TSseek_Config.SUBSEQUENCE_CASE}" else ""
    val datasetDir = s"query_results_${TSseek_Config.DATASET.toLowerCase}_${TSseek_Config.DATASET_SIZE}_${matchingType}${subCase}"
    val outDir = new File(s"/home/YOUR_USERNAME/results/$datasetDir/")
    if (!outDir.exists()) outDir.mkdirs()
    val path = s"/home/YOUR_USERNAME/results/$datasetDir/${tableIdentifier}_run${runIdx}_$timestamp.csv"
    val w = new BufferedWriter(new FileWriter(path))
    try {
      w.write("time_series_id\n")
      ids.foreach(id => w.write(s"$id\n"))
    } finally {
      w.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val DATASET = TSseek_Config.DATASET
    val DATASET_SIZE = TSseek_Config.DATASET_SIZE
    val ALPHA = TSseek_Config.ALPHA
    val MATCHING_TYPE = TSseek_Config.MATCHING_TYPE
    val SUBSEQUENCE_CASE = TSseek_Config.SUBSEQUENCE_CASE
    val appName = f"QueryProcessing_DesignSpace_${DATASET}_${DATASET_SIZE}_alpha${ALPHA}%.2f_${MATCHING_TYPE}_${SUBSEQUENCE_CASE}"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("spark://YOUR_SPARK_MASTER_HOST:7077")
      .set("spark.executor.memory", "32g")
      .set("spark.driver.memory", "64g")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memoryOverhead", "1g")
      .set("dfs.client.use.datanode.hostname", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/home/YOUR_USERNAME/spark-temp")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val SERIES_LENGTH = TSseek_Config.SERIES_LENGTH
    val mappingBasePath = TSseek_Config.mappingHdfsBase
    val s1HdfsStart = System.nanoTime()
    val gridCellCounts = readMapFromHDFS[String, Int](
      s"$mappingBasePath/gridCellAccumulator",
      spark, identity, _.toInt
    )
    val rawTableToHost = readMapFromHDFS[String, String](
      s"$mappingBasePath/tableToHostMapping",
      spark, identity, identity
    )
    val ipToHost = Map("YOUR_DB_SERVER1_IP" -> "shark1", "YOUR_DB_SERVER2_IP" -> "shark2")
    val tableToHost = rawTableToHost.map { case (fullTableName, host) =>
      fullTableName -> ipToHost.getOrElse(host, host)
    }

    val expectedPrefix = TSseek_Config.segmentsTablePrefix
    val expectedTables = TSseek_Config.numTables(DATASET_SIZE)
    val badKeys = tableToHost.keys.filterNot(k =>
      k.startsWith(expectedPrefix) && k.stripPrefix(expectedPrefix).nonEmpty && k.stripPrefix(expectedPrefix).forall(_.isDigit))
    require(badKeys.isEmpty,
      s"tableToHostMapping has ${badKeys.size} malformed key(s), e.g. '${badKeys.head}'; expected '${expectedPrefix}N'")
    require(tableToHost.size == expectedTables,
      s"tableToHostMapping has ${tableToHost.size} usable entries, expected $expectedTables")
    val indexParamsPath = s"$mappingBasePath/index_params"
    val storedIndexParams: Map[String, String] =
      try readMapFromHDFS[String, String](indexParamsPath, spark, identity, identity)
      catch { case _: Exception => Map.empty[String, String] }
    if (storedIndexParams.isEmpty) {
      println(s"!!! No index_params at $indexParamsPath; grid/threshold agreement NOT verified")
      require(SERIES_LENGTH == 128, s"an index without index_params was built for length 128, not TSSEEK_SERIES_LENGTH=$SERIES_LENGTH")
    } else {
      val qp = TSseek_Config.getParams()
      def stored(k: String): Double = storedIndexParams(k).toDouble
      require(stored("gridW") == qp.gridW && stored("gridH") == qp.gridH && stored("yMin") == qp.yMin && stored("yMax") == qp.yMax,
        s"index grid $storedIndexParams differs from query-side grid w=${qp.gridW} h=${qp.gridH} y=[${qp.yMin},${qp.yMax}]")
      require(qp.epsilon >= stored("threshold") - 1e-9,
        s"query epsilon ${qp.epsilon} is smaller than the index threshold ${stored("threshold")}; probes would be narrower than the stored approximation error")
      require(storedIndexParams.get("seriesLength").forall(_.toInt == SERIES_LENGTH),
        s"index built for series length ${storedIndexParams("seriesLength")}, query side has TSSEEK_SERIES_LENGTH=$SERIES_LENGTH")
      println(s"index_params verified against query-side config: $storedIndexParams")
    }
    val indexVerified = storedIndexParams.nonEmpty
    val s1HdfsElapsed = (System.nanoTime() - s1HdfsStart) / 1e9d
    println(f"PHASE_TIMING stage1_hdfs_metadata=$s1HdfsElapsed%.2f")
    val databases = Map(
      "shark1" -> "jdbc:postgresql://YOUR_SPARK_MASTER_HOST:5432/postgres",
      "shark2" -> "jdbc:postgresql://YOUR_DB_SERVER2_HOST:5432/postgres"
    )
    val params = TSseek_Config.getParams()
    val threshold = params.threshold
    val span90 = params.span90
    val gridW = params.gridW
    val gridH = params.gridH
    val yMin = params.yMin
    val yMax = params.yMax
    val epsilon = params.epsilon
    println(s"=== DATASET: $DATASET, SIZE: $DATASET_SIZE, MATCHING TYPE: $MATCHING_TYPE ===")
    if (MATCHING_TYPE == "SUBSEQUENCE") println(s"=== SUBSEQUENCE CASE: $SUBSEQUENCE_CASE ===")
    println(s"Threshold: $threshold, Span90%: $span90, Epsilon: $epsilon")
    println(s"Grid parameters: w=$gridW, h=$gridH, y-range=[$yMin, $yMax], series length=$SERIES_LENGTH")
    val filterSettings = TSseek_03_IndexContract.FilterSettings.fromConfig()
    println(s"Stage 1 filters: slope=${filterSettings.slopeMode}, legacy fluctuation=${filterSettings.legacyFluctuation}, step columns=${filterSettings.stepFilter}")
    val queryPattern = TSseek_Config.getPattern(DATASET, MATCHING_TYPE)
    println(s"Pattern: $queryPattern")
    val gridSpec = TSseek_03_IndexContract.GridSpec(gridW, gridH, yMin, yMax, SERIES_LENGTH)

    val cbdGridCells = (for (i <- 0 until gridSpec.xSteps; j <- 0 until gridSpec.ySteps) yield
      TSseek_03_CostBasedDecision.GridCell(gridSpec.cellKey(i, j), gridSpec.xBL(i), gridSpec.yBL(j),
        gridSpec.xBL(i) + gridW, gridSpec.yBL(j) + gridH)).toList

    def provablyEmpty(w: TSseek_03_IndexContract.ProbeWindow, count: Long): Boolean =
      count == 0 && indexVerified && TSseek_03_IndexContract.windowInsideGrid(gridSpec, w)

    def bandCost(p: TSseek_03_SubseqQueryParser.PatternSpec): Long =
      TSseek_03_IndexContract.windowCount(gridSpec, TSseek_03_IndexContract.ProbeWindow(0, SERIES_LENGTH - 1,
        TSseek_03_IndexContract.expandLow(p.yMin, epsilon), TSseek_03_IndexContract.expandHigh(p.yMax, epsilon)),
        gridCellCounts)
    val numRuns = 1
    val durations = ArrayBuffer[Double]()

    val batchSize = if (tableToHost.size > 160) 8
                   else if (tableToHost.size > 80)  16
                   else if (tableToHost.size > 40)  20
                   else 20

    if (MATCHING_TYPE == "SUBSEQUENCE") {
      println(s"=== SUBSEQUENCE MATCHING MODE — Case $SUBSEQUENCE_CASE ===")
      val results = mutable.Map[String, Set[Int]]()
      val subQuery = TSseek_03_SubseqQueryParser.parse(queryPattern, epsilon, filterSettings)
      println(s"Detected case: ${subQuery.detectedCase}, patterns: ${subQuery.patterns.size}, probes: ${subQuery.probes.size}")
      val subRewriteRules = TSseek_03_QueryWindowParser.RewriteRules.fromConfig()
      println(s"REWRITE_RULES=${subRewriteRules.name}")
      val r3Reason =
        if (subRewriteRules.conflict) TSseek_03_SubseqQueryParser.conflictReason(subQuery, SERIES_LENGTH) else None
      println(s"REWRITE_FIRED=${if (r3Reason.isDefined) "conflict" else "none"}")
      r3Reason.foreach(r => println(s"Stage 1 proves the answer empty: $r; no SQL issued"))

      val requiredWindows = subQuery.patterns.zipWithIndex.flatMap { case (p, i) =>
        subQuery.probes.lift(i) match {
          case Some(pr) => Seq(TSseek_03_SubseqQueryParser.probeToWindow(pr.leftProbe),
                               TSseek_03_SubseqQueryParser.probeToWindow(pr.rightProbe)).distinct
          case None => Seq(TSseek_03_IndexContract.ProbeWindow(0, SERIES_LENGTH - 1,
            TSseek_03_IndexContract.expandLow(p.yMin, epsilon), TSseek_03_IndexContract.expandHigh(p.yMax, epsilon)))
        }
      }
      val zeroWindow =
        if (r3Reason.isDefined) None
        else requiredWindows.map(w => (w, TSseek_03_IndexContract.windowCount(gridSpec, w, gridCellCounts)))
          .find { case (w, n) => provablyEmpty(w, n) }
      if (r3Reason.isEmpty)
        requiredWindows.foreach(w => println(s"Required window ${w.describe}: grid count ${TSseek_03_IndexContract.windowCount(gridSpec, w, gridCellCounts)}"))
      if (r3Reason.isDefined || zeroWindow.isDefined) {
        durations += 0.0
      } else SUBSEQUENCE_CASE match {

        case "1A" | "1B" | "2A"
           | "FIXED_START_FIXED_LEN" | "FIXED_START_DYN_LEN"
           | "BOUNDED_FIXED_LEN" | "BOUNDED_DYN_LEN" =>
          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case $SUBSEQUENCE_CASE ===")
            val t0 = System.currentTimeMillis()
            val costDecisionStartTime = System.currentTimeMillis()
            val (useSpatial, bestProbeIdx) = subQuery.detectedCase match {
              case TSseek_03_SubseqQueryParser.Case1A | TSseek_03_SubseqQueryParser.Case1B
                 | TSseek_03_SubseqQueryParser.CaseFixedStartFixedLen
                 | TSseek_03_SubseqQueryParser.CaseFixedStartDynLen
                 | TSseek_03_SubseqQueryParser.CaseBoundedFixedLen
                 | TSseek_03_SubseqQueryParser.CaseBoundedDynLen =>
                val probes = subQuery.probes.head
                val spatialCost = TSseek_03_CostBasedDecision.estimateSpatialIntersectCost(
                  gridCellCounts, cbdGridCells, probes)
                val btreeCost = bandCost(probes.pattern)
                val decision = spatialCost <= btreeCost
                println(s"Cost estimate: spatial=$spatialCost, btree=$btreeCost → ${if (decision) "SPATIAL" else "B-TREE"}")
                (decision, 0)
              case TSseek_03_SubseqQueryParser.Case2A =>
                val configStrategy = TSseek_Config.MULTI_PATTERN_STRATEGY
                println(s"Multi-pattern strategy (from config): $configStrategy")
                configStrategy match {
                  case "INTERSECT_ALL" =>
                    println(s"INTERSECT-ALL: spatial queries for ALL ${subQuery.probes.size} patterns")
                    (true, -1)
                  case "BEST_ONLY" =>
                    val bestIdx = TSseek_03_SubseqQueryParser.selectMostSelectivePattern(subQuery.patterns)
                    val bestProbes = subQuery.probes(bestIdx)
                    val spatialCost = TSseek_03_CostBasedDecision.estimateSpatialIntersectCost(
                      gridCellCounts, cbdGridCells, bestProbes)
                    val btreeCost = bandCost(bestProbes.pattern)
                    val decision = spatialCost <= btreeCost
                    println(s"BEST-ONLY: pattern index $bestIdx, spatial=$spatialCost, btree=$btreeCost → ${if (decision) "SPATIAL" else "B-TREE"}")
                    (decision, bestIdx)
                  case "CASCADE" =>
                    println(s"CASCADE: will process patterns sequentially")
                    (true, -2)
                  case _ =>
                    throw new IllegalArgumentException(s"Unknown MULTI_PATTERN_STRATEGY: $configStrategy")
                }
              case _ => (false, 0)
            }
            val costDecisionEndTime = System.currentTimeMillis()
            println(s"Cost-based decision took ${(costDecisionEndTime - costDecisionStartTime) / 1000.0}s")

            val queryDistributionStartTime = System.currentTimeMillis()
            println(s"Processing ${tableToHost.size} tables in batches of $batchSize")
            val tableList = tableToHost.toList
            results.clear()
            if (bestProbeIdx == -2) {
              val rankedIndices = TSseek_03_SubseqQueryParser.rankPatternsBySelectivity(subQuery.patterns)
              val cascadeBatchSize = 50000
              println(s"CASCADE: processing patterns in selectivity order: ${rankedIndices.mkString(" → ")}")
              for ((table, host) <- tableList) {
                var candidateIds: Set[Int] = null
                for (rank <- rankedIndices.indices) {
                  val patIdx = rankedIndices(rank)
                  if (candidateIds == null) {

                    val sql = TSseek_03_SubseqQueryParser.generateSpatialIntersectSql(table, subQuery.probes(patIdx))
                    val ids = Await.result(
                      executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                      Duration.Inf)
                    candidateIds = ids
                    println(s"[$table] Pattern $patIdx (first): ${candidateIds.size} candidates")
                  } else if (candidateIds.nonEmpty) {
                    val candidateArray = candidateIds.toArray.map(_.toLong)
                    val newCandidates = mutable.Set[Int]()
                    for (chunk <- candidateArray.grouped(cascadeBatchSize)) {
                      val sql = TSseek_03_SubseqQueryParser.generateCascadeNextSpatialSql(
                        table, subQuery.probes(patIdx), chunk)
                      val chunkIds = Await.result(
                        executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                        Duration.Inf)
                      newCandidates ++= chunkIds
                    }
                    candidateIds = newCandidates.toSet
                    println(s"[$table] Pattern $patIdx (cascade): ${candidateIds.size} candidates remaining")
                  }
                }
                val finalIds = if (candidateIds != null) candidateIds else Set.empty[Int]
                val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, finalIds, runNum, ts)
                results += table -> finalIds
              }
            } else {
              for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                val futures = if (useSpatial && bestProbeIdx == -1) {
                  for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateMultiPatternSpatialIntersectSql(table, subQuery.probes)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                } else if (useSpatial) {
                  val chosenProbes = subQuery.probes(bestProbeIdx)
                  for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateSpatialIntersectSql(table, chosenProbes)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                } else {

                  val pattern = if (bestProbeIdx >= 0) subQuery.patterns(bestProbeIdx) else subQuery.patterns.head
                  for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateBtreeSql(table, pattern, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                }
                val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                  .groupBy(_._1)
                  .map { case (table, lists) =>
                    table -> lists.flatMap(_._2).toSet
                  }
                val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                batchResults.foreach { case (table, ids) =>
                  if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts)
                }
                results ++= batchResults.map { case (table, ids) =>
                  table -> ids
                }
                println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")
              }
            }
            val queryDistributionEndTime = System.currentTimeMillis()
            println(s"Query distribution and execution took ${(queryDistributionEndTime - queryDistributionStartTime) / 1000.0}s")
            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case $SUBSEQUENCE_CASE run took $runSec%.2f seconds")
            durations += runSec
          }

        case "1C" | "FREE_FIXED_LEN" | "FREE_DYN_LEN" | "FREE_DYN_LEN_WIDE" | "STEP_INCREASE" | "STEP_FLAT" =>
          val pattern = subQuery.patterns.head
          println(s"Parsed pattern: direction=${pattern.direction}, domain=${pattern.domain}, length=[${pattern.minLength},${pattern.maxLength}]")

          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case 1C (B-tree only) ===")
            val t0 = System.currentTimeMillis()

            println(s"Processing ${tableToHost.size} tables in batches of $batchSize with SAFE filters (no spatial query)")
            val tableList = tableToHost.toList
            results.clear()

            for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
              val futures = for {
                (table, host) <- batch
              } yield {
                val sql = TSseek_03_SubseqQueryParser.generateBtreeSql(table, pattern, epsilon)
                executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                  .map(ids => (table, ids))
              }

              val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                .groupBy(_._1)
                .map { case (table, lists) =>
                  table -> lists.flatMap(_._2).toSet
                }
              val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
              batchResults.foreach { case (table, ids) =>
                if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts)
              }
              results ++= batchResults.map { case (table, ids) =>
                table -> ids
              }
              println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${results.size}/${tableToHost.size} tables processed")
            }
            println(s"All ${tableToHost.size} tables processed for Case 1C")
            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case 1C run took $runSec%.2f seconds")
            durations += runSec
          }

        case "2B" | "2C"
           | "MULTI_ADJACENT" | "MULTI_FIXED_GAP" | "MULTI_RANGE_GAP"
           | "MULTI_ORDERED_FLEX_GAP" | "MULTI_UNORDERED_FLEX_GAP" =>
          val strategy = TSseek_Config.MULTI_PATTERN_STRATEGY
          println(s"Multi-pattern strategy: $strategy")
          if (subQuery.gaps.nonEmpty) {
            val gapStr = subQuery.gaps.map { g =>
              if (g.isFixed) s"#${g.minLength}#" else s"#${g.minLength}-${g.maxLength}#"
            }.mkString(", ")
            println(s"Gaps: $gapStr")
          }
          if (subQuery.unordered) println(s"UNORDERED: patterns may appear in any order")
          val rankedIndices = TSseek_03_SubseqQueryParser.rankPatternsBySelectivity(subQuery.patterns)
          println(s"Pattern selectivity ranking: ${rankedIndices.mkString(" → ")}")
          for (runNum <- 1 to numRuns) {
            println(s"=== RUN #${runNum}/1: Case $SUBSEQUENCE_CASE ($strategy) ===")
            val t0 = System.currentTimeMillis()

            val tableList = tableToHost.toList
            results.clear()
            strategy match {
              case "INTERSECT_ALL" =>
                println(s"Processing ${tableToHost.size} tables with multi-pattern B-tree INTERSECT")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateMultiPatternBtreeIntersectSql(
                      table, subQuery.patterns, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }
                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }

                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                }

              case "COMPOSITE" =>
                println(s"Processing ${tableToHost.size} tables with multi-pattern COMPOSITE (single-scan GROUP BY + HAVING)")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateMultiPatternCompositeSql(
                      table, subQuery.patterns, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }
                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }
                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                }
              case "BEST_ONLY" =>
                val bestIdx = TSseek_03_SubseqQueryParser.selectMostSelectivePattern(subQuery.patterns)
                println(s"BEST_ONLY: using pattern index $bestIdx (most selective)")
                for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
                  val futures = for ((table, host) <- batch) yield {
                    val sql = TSseek_03_SubseqQueryParser.generateBestOnlyBtreeSql(
                      table, subQuery.patterns, bestIdx, epsilon)
                    executeDesignSpaceSql(table, host, databases, sql, threshold, span90)
                      .map(ids => (table, ids))
                  }
                  val batchResults = Await.result(Future.sequence(futures), Duration.Inf)
                    .groupBy(_._1).map { case (table, lists) => table -> lists.flatMap(_._2).toSet }
                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  batchResults.foreach { case (table, ids) => if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts) }
                  results ++= batchResults.map { case (table, ids) => table -> ids }

                  println(s"Batch ${batchIdx + 1} completed: ${results.size}/${tableToHost.size} tables")
                }

              case "CASCADE" =>
                println(s"CASCADE: processing patterns in selectivity order: ${rankedIndices.mkString(" → ")}")
                val cascadeBatchSize = 50000
                for ((table, host) <- tableList) {
                  var candidateIds: Set[Int] = null
                  for (rank <- rankedIndices.indices) {
                    val patIdx = rankedIndices(rank)
                    val pattern = subQuery.patterns(patIdx)
                    if (candidateIds == null) {
                      val sql = TSseek_03_SubseqQueryParser.generateCascadeFirstBtreeSql(table, pattern, epsilon)
                      val ids = Await.result(
                        executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                        Duration.Inf)
                      candidateIds = ids
                      println(s"[$table] Pattern $patIdx (first): ${candidateIds.size} candidates")
                    } else if (candidateIds.nonEmpty) {
                      val candidateArray = candidateIds.toArray.map(_.toLong)
                      val newCandidates = mutable.Set[Int]()
                      for (chunk <- candidateArray.grouped(cascadeBatchSize)) {
                        val sql = TSseek_03_SubseqQueryParser.generateCascadeNextBtreeSql(
                          table, pattern, epsilon, chunk)
                        val chunkIds = Await.result(
                          executeDesignSpaceSql(table, host, databases, sql, threshold, span90),
                          Duration.Inf)
                        newCandidates ++= chunkIds
                      }

                      candidateIds = newCandidates.toSet
                      println(s"[$table] Pattern $patIdx (cascade): ${candidateIds.size} candidates remaining")
                    }
                  }
                  val finalIds = if (candidateIds != null) candidateIds else Set.empty[Int]
                  val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
                  if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, finalIds, runNum, ts)
                  results += table -> finalIds
                  if (results.size % batchSize == 0) {
                    println(s"Progress: ${results.size}/${tableToHost.size} tables")
                  }
                }
              case _ =>
                throw new IllegalArgumentException(s"Unknown MULTI_PATTERN_STRATEGY: $strategy")
            }
            println(s"All ${tableToHost.size} tables processed for Case $SUBSEQUENCE_CASE ($strategy)")
            val t1 = System.currentTimeMillis()
            val runSec = (t1 - t0) / 1000.0
            println(f"Case $SUBSEQUENCE_CASE run took $runSec%.2f seconds")
            durations += runSec
          }
        case _ =>
          throw new IllegalArgumentException(s"Unknown SUBSEQUENCE_CASE: $SUBSEQUENCE_CASE")
      }

      val s1SqlElapsed = durations.sum
      println(f"PHASE_TIMING stage1_sql=$s1SqlElapsed%.2f")
      println(f"PHASE_TIMING stage1_total=${s1HdfsElapsed + s1SqlElapsed}%.2f")

      val refinementOutputBase = DATASET match {
        case "TSBS"       => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs"
        case "ECG"        => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg"
        case "RANDOMWALK" => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk"
        case _            => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/${DATASET.toLowerCase}"
      }

      val groundTruthSeeds: Set[Long] = TSseek_Config.getGroundTruthSeeds(DATASET)
      r3Reason.orElse(zeroWindow.map { case (w, _) => s"zero grid count for window ${w.describe}" }) match {
        case Some(reason) =>
          TSseek_05_RetrievalRefinement.writeShortCircuitResult(spark,
            TSseek_05_RetrievalRefinement.subsequenceOutputPath(refinementOutputBase), reason)
        case None =>
          println("\n" + "=" * 80)
          println("=== FUSED STAGE 2 STARTING (same SparkContext as Stage 1) ===")
          println("=" * 80)
          val fusedStage2Start = System.currentTimeMillis()
          TSseek_05_RetrievalRefinement.runSubsequenceRefinementInline(
            spark,
            results.toMap,
            queryPattern,
            refinementOutputBase,
            groundTruthSeeds
          )
          val fusedStage2End = System.currentTimeMillis()
          val fusedStage2Wall = (fusedStage2End - fusedStage2Start) / 1000.0
          println(f"FUSED STAGE 2 wall: $fusedStage2Wall%.2f seconds")
          println(f"PHASE_TIMING stage2_wall=$fusedStage2Wall%.2f")
      }
    } else if (MATCHING_TYPE == "WHOLE_SEQUENCE") {

      println("=== WHOLE-SEQUENCE MATCHING MODE (conservative probes, INTERSECT) ===")
      val rewriteRules = TSseek_03_QueryWindowParser.RewriteRules.fromConfig()
      println(s"REWRITE_RULES=${rewriteRules.name}")
      val countOf = (w: TSseek_03_IndexContract.ProbeWindow) => TSseek_03_IndexContract.windowCount(gridSpec, w, gridCellCounts)
      val wholeResults = mutable.Map[String, Set[Int]]()
      var shortCircuit: Option[String] = None
      for (runNum <- 1 to numRuns) {
        println(s"=== RUN #${runNum}/1: pattern = $queryPattern")
        wholeResults.clear()
        shortCircuit = None
        val t0 = System.currentTimeMillis()
        val queryParsingStartTime = System.currentTimeMillis()
        val wholeQuery = TSseek_03_QueryWindowParser.parseWholeQuery(queryPattern)
        val plan = TSseek_03_QueryWindowParser.planWholeProbesWithRules(wholeQuery, epsilon, SERIES_LENGTH, filterSettings,
          rewriteRules, countOf)
        val maxTotal = if (wholeQuery.maxTotal == Long.MaxValue) "unbounded" else wholeQuery.maxTotal.toString
        println(s"Whole query: ${wholeQuery.constructs.size} constructs, length ${wholeQuery.minTotal}..$maxTotal, " +
          s"implicit trailing wildcard: ${wholeQuery.needsImplicitTail(SERIES_LENGTH)}")
        println(s"REWRITE_FIRED=${if (plan.fired.isEmpty) "none" else plan.fired.mkString(",")}")
        if (plan.fired.contains("tighten:lengths"))
          println("Tightened lengths: " + plan.resolved.indices.collect {
            case i if plan.lengths(i) != ((plan.resolved(i).minLen, plan.resolved(i).maxLen)) =>
              val w = plan.resolved(i)
              s"construct $i {${w.minLen},${if (w.maxLen == TSseek_03_QueryWindowParser.Unbounded) "*" else w.maxLen}} -> " +
                s"{${plan.lengths(i)._1},${plan.lengths(i)._2}}"
          }.mkString("; "))
        println(s"Window query parsing took ${(System.currentTimeMillis() - queryParsingStartTime) / 1000.0} seconds")
        val querySelectionStartTime = System.currentTimeMillis()
        if (!plan.feasible) {
          shortCircuit = Some(plan.infeasibleReason.getOrElse(s"no allocation of construct lengths covers $SERIES_LENGTH readings"))
        } else {
          println("=== Window Query Intersection Counts ===")
          val selection = TSseek_03_QueryWindowParser.selectWholeCandidate(plan, countOf)
          selection.counts.foreach { case (c, counts) =>
            if (!c.superset) println(TSseek_03_QueryWindowParser.constructLogLine(c, counts))
            c.windows.zip(counts).find { case (w, n) => provablyEmpty(w, n) }.foreach { case (w, _) =>
              if (shortCircuit.isEmpty) shortCircuit = Some(s"zero grid count for ${c.label} window ${w.describe}")
            }
          }
          if (plan.supersets.nonEmpty)
            println(TSseek_03_QueryWindowParser.supersetLogLine(selection.counts.filter(_._1.superset)))
          println("=========================================")
          println(s"!!!!Window Query Selection Time: ${(System.currentTimeMillis() - querySelectionStartTime) / 1000.0} seconds.")
          if (shortCircuit.isEmpty) {
            val queryDistributionStartTime = System.currentTimeMillis()
            val chosen = selection.chosen
            chosen match {
              case Some((c, est)) =>
                println(TSseek_03_QueryWindowParser.chosenLogLine(c, est))
                println(s"PLAN_CHOSEN rules=${plan.rules} kind=${if (c.superset) "superset" else "construct"} index=${c.index} " +
                  s"estimate=$est windows=${c.windows.map(_.describe).mkString("; ")} filters=${c.filters.size}")
              case None =>
                println("No probe candidates (all constructs are wildcards, or none can start): every series of the right length is a candidate")
            }
            println(s"Distributing across ${tableToHost.size} tables in batches of $batchSize")
            val tableList = tableToHost.toList
            for ((batch, batchIdx) <- tableList.grouped(batchSize).zipWithIndex) {
              val futures = for ((table, host) <- batch) yield {
                val sql = chosen match {
                  case Some((c, _)) => wholeProbeSql(table, c)
                  case None => s"SELECT DISTINCT time_series_id FROM $table"
                }
                executeDesignSpaceSql(table, host, databases, sql, threshold, span90).map(ids => (table, ids))
              }
              val batchResults = Await.result(Future.sequence(futures), Duration.Inf).toMap

              val ts = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
              batchResults.foreach { case (table, ids) =>
                if (TSseek_Config.SAVE_CANDIDATES) saveIdsToCsv(table, ids, runNum, ts)
              }
              wholeResults ++= batchResults
              println(s"Batch ${batchIdx + 1}/${Math.ceil(tableList.size.toDouble / batchSize).toInt} completed: ${wholeResults.size}/${tableToHost.size} tables processed")
            }
            println(s"All ${tableToHost.size} tables processed with INTERSECT")
            println(s"Enhanced Query Distribution and Execution Time: ${(System.currentTimeMillis() - queryDistributionStartTime) / 1000.0} seconds")
          }
        }
        shortCircuit.foreach(r => println(s"Stage 1 proves the answer empty: $r; no SQL issued"))

        val t1 = System.currentTimeMillis()
        val runSec = (t1 - t0) / 1000.0
        println(f"Whole-sequence matching run took $runSec%.2f seconds")
        durations += runSec
      }
      println(f"PHASE_TIMING stage1_sql=${durations.sum}%.2f")
      println(f"PHASE_TIMING stage1_total=${s1HdfsElapsed + durations.sum}%.2f")

      val refinementOutputBase = DATASET match {
        case "TSBS"       => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/tsbs"
        case "ECG"        => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/ecg"
        case "RANDOMWALK" => "hdfs://shark1local:9000/user/YOUR_USERNAME/output/random_walk"
        case _            => s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/${DATASET.toLowerCase}"
      }
      val groundTruthSeeds: Set[Long] = TSseek_Config.getGroundTruthSeeds(DATASET)
      shortCircuit match {
        case Some(reason) =>
          TSseek_05_RetrievalRefinement.writeShortCircuitResult(spark,
            TSseek_05_RetrievalRefinement.wholeOutputPath(refinementOutputBase), reason)
        case None =>
          println("\n" + "=" * 80)
          println("=== FUSED STAGE 2 (WHOLE) STARTING (same SparkContext as Stage 1) ===")
          println("=" * 80)
          val fusedWholeStart = System.currentTimeMillis()
          TSseek_05_RetrievalRefinement.runWholeRefinementInline(
            spark,
            wholeResults.toMap,
            queryPattern,
            refinementOutputBase,
            groundTruthSeeds
          )
          val fusedWholeWall = (System.currentTimeMillis() - fusedWholeStart) / 1000.0
          println(f"FUSED STAGE 2 (WHOLE) wall: $fusedWholeWall%.2f seconds")
          println(f"PHASE_TIMING stage2_wall=$fusedWholeWall%.2f")
      }
    } else {
      throw new IllegalArgumentException(s"Unknown MATCHING_TYPE: $MATCHING_TYPE")
    }
    val total = durations.sum
    val avgTime = total / numRuns
    val minTime = durations.min
    val maxTime = durations.max
    println(f"All $numRuns runs finished in ${total}%.2f seconds")
    println(f"Average time per run: ${avgTime}%.3f seconds")
    println(f"Min/Max time: ${minTime}%.3f / ${maxTime}%.3f seconds")
    spark.stop()
  }
}
