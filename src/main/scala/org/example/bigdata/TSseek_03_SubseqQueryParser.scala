package org.example.bigdata

import org.example.bigdata.TSseek_03_IndexContract.{FilterSettings, ProbeWindow, SegmentFilter, patternFilters}
import org.example.bigdata.TSseek_03_QueryLanguage._

object TSseek_03_SubseqQueryParser {
  sealed trait PositionConstraint extends Serializable
  case class FixedStart(start: Int) extends PositionConstraint
  case class BoundedRange(rangeStart: Int, rangeEnd: Int) extends PositionConstraint
  case object FreePosition extends PositionConstraint
  case class PatternSpec(
    yStart: Double,
    yEnd: Double,
    direction: String,
    stepSize: Double,
    minLength: Int,
    maxLength: Int,
    position: PositionConstraint,
    domain: Domain = Domain(Double.NegativeInfinity, lowInclusive = false, Double.PositiveInfinity, highInclusive = false),
    magnitude: Magnitude = SignOnly,
    filters: Vector[SegmentFilter] = Vector.empty
  ) {
    def isFixedLength: Boolean = minLength == maxLength
    def isDynamicLength: Boolean = minLength != maxLength
    def yMin: Double = math.min(yStart, yEnd)
    def yMax: Double = math.max(yStart, yEnd)
    def filterSql: String = filters.map(_.sql).mkString
    def guaranteedExtent: Option[(Int, Int)] = position match {
      case FixedStart(s) => Some((s, s + minLength - 1))
      case BoundedRange(a, b) if b <= a + minLength - 1 => Some((b, a + minLength - 1))
      case _ => None
    }
  }
  case class Gap(minLength: Int, maxLength: Int) {
    def isFixed: Boolean = minLength == maxLength
    def isAdjacent: Boolean = minLength == 0 && maxLength == 0
    def isFlexible: Boolean = maxLength == Int.MaxValue
    def length: Int = minLength
  }
  object Gap {
    def apply(fixed: Int): Gap = Gap(fixed, fixed)
    val Flexible: Gap = Gap(0, Int.MaxValue)
  }

  sealed trait ProbeGeometry extends Serializable
  case class LineProbe(x: Double, yMin: Double, yMax: Double) extends ProbeGeometry
  case class EnvelopeProbe(xMin: Double, yMin: Double, xMax: Double, yMax: Double) extends ProbeGeometry
  case class PatternProbes(
    leftProbe: ProbeGeometry,
    rightProbe: ProbeGeometry,
    pattern: PatternSpec
  )
  sealed trait SubsequenceCase extends Serializable
  case object Case1A extends SubsequenceCase
  case object Case1B extends SubsequenceCase
  case object Case1C extends SubsequenceCase
  case object Case2A extends SubsequenceCase
  case object Case2B extends SubsequenceCase
  case object Case2C extends SubsequenceCase
  case object CaseFreeFixedLen extends SubsequenceCase
  case object CaseFreeDynLen extends SubsequenceCase
  case object CaseBoundedFixedLen extends SubsequenceCase
  case object CaseBoundedDynLen extends SubsequenceCase
  case object CaseFixedStartFixedLen extends SubsequenceCase
  case object CaseFixedStartDynLen extends SubsequenceCase
  case object CaseMultiAdjacent extends SubsequenceCase
  case object CaseMultiFixedGap extends SubsequenceCase
  case object CaseMultiRangeGap extends SubsequenceCase
  case object CaseMultiOrderedFlexGap extends SubsequenceCase
  case object CaseMultiUnorderedFlexGap extends SubsequenceCase
  case class SubsequenceQuery(
    detectedCase: SubsequenceCase,
    patterns: List[PatternSpec],
    gaps: List[Gap],
    globalBound: Option[BoundedRange],
    threshold: Double,
    probes: List[PatternProbes],
    unordered: Boolean = false,
    thresholdPercent: Boolean = false
  )
  val DefaultFilterSettings: FilterSettings = FilterSettings("safe", legacyFluctuation = false, stepFilter = false)
  def parse(queryString: String, epsilon: Double, settings: FilterSettings = DefaultFilterSettings): SubsequenceQuery = {
    val lexed = lexSubsequence(queryString)
    val threshold = lexed.threshold
    val anchors = lexed.blocks.map(_.anchor)
    def spec(fields: Seq[String], position: PositionConstraint): PatternSpec = {
      val domain = parseDomain(fields(0))
      val direction = parseDirection(fields(1))
      val magnitude = parseMagnitude(fields(2), direction)
      val (minLen, maxLen) = parseLength(fields(3))
      val stepSize = magnitude match { case StepMagnitude(m, _, _, _, true) => m; case _ => 0.0 }
      val base = PatternSpec(domain.low, domain.high, direction, stepSize, minLen, maxLen, position, domain, magnitude)
      base.copy(filters = patternFilters(direction, magnitude, threshold, domain, minLen, base.guaranteedExtent,
        epsilon, settings, unanchored = position == FreePosition))
    }
    def query(c: SubsequenceCase, patterns: List[PatternSpec], gaps: List[Gap], bound: Option[BoundedRange],
              probes: List[PatternProbes], unordered: Boolean = false): SubsequenceQuery =
      SubsequenceQuery(c, patterns, gaps, bound, threshold.value, probes, unordered, threshold.percent)

    if (lexed.blocks.size == 1) {
      val fields = lexed.blocks.head.fields
      anchors.head match {
        case Some(AtPosition(s)) =>
          val pattern = spec(fields, FixedStart(s))
          val c = if (pattern.isFixedLength) CaseFixedStartFixedLen else CaseFixedStartDynLen
          query(c, List(pattern), Nil, None, List(generateProbes1A(pattern, epsilon)))
        case Some(AtRange(a, b)) =>
          val pattern = spec(fields, BoundedRange(a, b))
          val c = if (pattern.isFixedLength) CaseBoundedFixedLen else CaseBoundedDynLen
          query(c, List(pattern), Nil, None, List(generateProbes1B(pattern, epsilon)))
        case None =>
          val pattern = spec(fields, FreePosition)
          val c = if (pattern.isFixedLength) CaseFreeFixedLen else CaseFreeDynLen
          query(c, List(pattern), Nil, None, Nil)
      }
    } else if (anchors.forall(_.exists(_.isInstanceOf[AtPosition]))) {
      if (!lexed.separators.forall(_ == AdjacentSeparator))
        throw new QuerySyntaxException(s"fixed @N positions take plain commas between blocks: '$queryString'")
      val patterns = lexed.blocks.map(b => spec(b.fields, FixedStart(b.anchor.get.asInstanceOf[AtPosition].position))).toList
      val rewritten = applyOverlapRewriting(patterns)
      query(Case2A, rewritten, Nil, None, rewritten.map(p => generateProbes1A(p, epsilon)))
    } else {

      val globalBound = anchors.last match {
        case Some(AtRange(a, b)) if anchors.init.forall(_.isEmpty) => Some(BoundedRange(a, b))
        case None if anchors.forall(_.isEmpty) => None
        case _ => throw new QuerySyntaxException(
          s"multi-pattern anchors must be @N on every block or a single @[a,b] after the last block: '$queryString'")
      }
      val patterns = lexed.blocks.map(b => spec(b.fields, FreePosition)).toList
      val gaps = lexed.separators.map {
        case AdjacentSeparator => Gap(0, 0)
        case GapSeparator(min, max) => Gap(min, max)
        case FlexSeparator => Gap.Flexible
      }.toList
      val detectedCase =
        if (lexed.unordered) CaseMultiUnorderedFlexGap
        else if (gaps.forall(_.isFlexible)) CaseMultiOrderedFlexGap
        else if (gaps.forall(_.isAdjacent)) CaseMultiAdjacent
        else if (gaps.forall(g => g.isFixed && !g.isAdjacent)) CaseMultiFixedGap
        else CaseMultiRangeGap
      query(detectedCase, patterns, gaps, globalBound, Nil, lexed.unordered)
    }
  }
  private def r3Tolerance(p: PatternSpec, threshold: Double, percent: Boolean): Double =
    if (!percent) threshold
    else p.magnitude match {
      case StepMagnitude(lo, _, hi, _, scalar) => math.abs(if (scalar) lo else (lo + hi) / 2.0) * (threshold / 100.0)
      case _ => 0.0
    }
  def conflictReason(q: SubsequenceQuery, seriesLength: Int): Option[String] = {
    val contradiction = q.patterns.iterator.zipWithIndex.flatMap { case (p, i) =>
      TSseek_03_QueryWindowParser.stepContradiction(p.direction, p.magnitude, p.domain, p.minLength,
        r3Tolerance(p, q.threshold, q.thresholdPercent)).map(why => s"contradiction in sub-pattern $i: $why")
    }.nextOption()
    def earliestStart(p: PatternSpec): Int = p.position match {
      case FixedStart(s) => s
      case BoundedRange(a, _) => a
      case FreePosition => 0
    }
    lazy val patternTooLong = q.patterns.iterator.zipWithIndex.collectFirst {
      case (p, i) if earliestStart(p).toLong + p.minLength > seriesLength =>
        s"length conflict: sub-pattern $i needs ${p.minLength} readings from position ${earliestStart(p)}, " +
          s"but series have $seriesLength"
    }
    lazy val chainTooLong =
      if (q.patterns.size < 2 || q.detectedCase == Case2A) None
      else {
        val minLengths = q.patterns.map(_.minLength.toLong).sum
        val minGaps = if (q.unordered) 0L else q.gaps.map(_.minLength.toLong).sum
        val from = q.globalBound.map(_.rangeStart.toLong).getOrElse(0L)
        if (from + minLengths + minGaps > seriesLength)
          Some(s"length conflict: the sub-patterns need at least ${minLengths + minGaps} readings " +
            s"(lengths $minLengths, gaps $minGaps) from position $from, but series have $seriesLength")
        else None
      }
    contradiction.orElse(patternTooLong).orElse(chainTooLong)
  }

  private def lowY(p: PatternSpec, epsilon: Double): Double = TSseek_03_IndexContract.expandLow(p.yMin, epsilon)
  private def highY(p: PatternSpec, epsilon: Double): Double = TSseek_03_IndexContract.expandHigh(p.yMax, epsilon)
  def generateProbes1A(pattern: PatternSpec, epsilon: Double): PatternProbes = {
    val start = pattern.position match {
      case FixedStart(s) => s
      case _ => throw new IllegalArgumentException("1A requires FixedStart position")
    }
    val yLow = lowY(pattern, epsilon)
    val yHigh = highY(pattern, epsilon)
    val leftProbe = LineProbe(start.toDouble, yLow, yHigh)
    val rightX = start + pattern.minLength - 1
    val rightProbe = if (rightX == start) leftProbe else LineProbe(rightX.toDouble, yLow, yHigh)
    PatternProbes(leftProbe = leftProbe, rightProbe = rightProbe, pattern = pattern)
  }

  def generateProbes1B(pattern: PatternSpec, epsilon: Double): PatternProbes = {
    val (a, b) = pattern.position match {
      case BoundedRange(start, end) => (start, end)
      case _ => throw new IllegalArgumentException("1B requires BoundedRange position")
    }
    val envelope = EnvelopeProbe(a.toDouble, lowY(pattern, epsilon), b.toDouble, highY(pattern, epsilon))
    PatternProbes(
      leftProbe = envelope,
      rightProbe = envelope,
      pattern = pattern
    )
  }
  def applyOverlapRewriting(patterns: List[PatternSpec]): List[PatternSpec] = {
    if (patterns.length <= 1) return patterns
    val result = patterns.toBuffer
    for (i <- 0 until result.length - 1) {
      val p1 = result(i)
      val p2 = result(i + 1)

      (p1.position, p2.position) match {
        case (FixedStart(s1), FixedStart(s2)) =>
          val p1MaxEnd = s1 + p1.maxLength - 1
          val p2Start = s2
          if (p1MaxEnd >= p2Start && p1.isDynamicLength) {
            val cappedMaxLen = p2Start - s1
            if (cappedMaxLen >= p1.minLength) {
              result(i) = p1.copy(maxLength = cappedMaxLen)
            }
          }
        case _ =>
      }
    }
    result.toList
  }

  private def probeWindow(probe: ProbeGeometry): ProbeWindow = probe match {
    case LineProbe(x, yMin, yMax) => ProbeWindow(x, x, yMin, yMax)
    case EnvelopeProbe(xMin, yMin, xMax, yMax) => ProbeWindow(xMin, xMax, yMin, yMax)
  }
  def probeToSql(probe: ProbeGeometry): String = TSseek_03_IndexContract.probeGeometrySql(probeWindow(probe))
  def probeToWindow(probe: ProbeGeometry): ProbeWindow = probeWindow(probe)
  def valueRangeConstraintSql(yMin: Double, yMax: Double, epsilon: Double): String =
    TSseek_03_IndexContract.yBandSql(TSseek_03_IndexContract.expandLow(yMin, epsilon), TSseek_03_IndexContract.expandHigh(yMax, epsilon))

  def generateSpatialIntersectSql(tableName: String, probes: PatternProbes): String = {
    val leftSql = probeToSql(probes.leftProbe)
    val rightSql = probeToSql(probes.rightProbe)
    val p = probes.pattern
    val band = probeWindow(probes.leftProbe)
    val yRange = TSseek_03_IndexContract.yBandSql(band.y0, band.y1)
    val filters = p.filterSql
    if (probes.leftProbe == probes.rightProbe) {
      s"""SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $leftSql)$yRange$filters""".stripMargin
    } else {
      s"""SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $leftSql)$yRange$filters
         |INTERSECT
         |SELECT DISTINCT time_series_id FROM $tableName
         |WHERE ST_Intersects(time_series_segments, $rightSql)$yRange$filters""".stripMargin
    }
  }

  def generateBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double): String = {
    val valueRange = valueRangeConstraintSql(pattern.yMin, pattern.yMax, epsilon)
    s"""SELECT DISTINCT time_series_id FROM $tableName
       |WHERE 1=1$valueRange${pattern.filterSql}""".stripMargin
  }
  def generateMultiPatternBtreeIntersectSql(tableName: String, patterns: List[PatternSpec], epsilon: Double): String = {
    patterns.map(p => generateBtreeSql(tableName, p, epsilon)).mkString("\nINTERSECT\n")
  }

  def predicateForPattern(pattern: PatternSpec, epsilon: Double): String = {
    val clause = (valueRangeConstraintSql(pattern.yMin, pattern.yMax, epsilon) + pattern.filterSql).stripPrefix(" AND ")
    if (clause.isEmpty) "TRUE" else clause
  }
  def generateMultiPatternCompositeSql(tableName: String, patterns: List[PatternSpec], epsilon: Double): String = {
    val preds = patterns.map(p => predicateForPattern(p, epsilon))
    val whereClause = preds.map(p => s"($p)").mkString(" OR ")
    val havingClause = preds.map(p => s"bool_or($p)").mkString(" AND ")
    s"""SELECT time_series_id
       |FROM $tableName
       |WHERE $whereClause
       |GROUP BY time_series_id
       |HAVING $havingClause""".stripMargin
  }
  def generateMultiPatternSpatialIntersectSql(tableName: String, probesList: List[PatternProbes]): String = {
    probesList.map(probes => generateSpatialIntersectSql(tableName, probes)).mkString("\nINTERSECT\n")
  }

  def estimateSelectivity(pattern: PatternSpec): Double = {
    val valueRangeWidth = math.abs(pattern.yMax - pattern.yMin)
    val dirFactor = pattern.direction match {
      case "INCREASING" | "DECREASING" => 0.5
      case "FLAT" => 0.7
      case _ => 1.0
    }

    val rangeFactor = math.min(valueRangeWidth / 5.0, 1.0)
    dirFactor * rangeFactor
  }
  def rankPatternsBySelectivity(patterns: List[PatternSpec]): List[Int] = {
    patterns.zipWithIndex
      .sortBy { case (p, _) => estimateSelectivity(p) }
      .map(_._2)
  }
  def selectMostSelectivePattern(patterns: List[PatternSpec]): Int = {
    rankPatternsBySelectivity(patterns).head
  }
  def generateBestOnlySpatialSql(tableName: String, probesList: List[PatternProbes], bestIdx: Int): String = {
    generateSpatialIntersectSql(tableName, probesList(bestIdx))
  }
  def generateBestOnlyBtreeSql(tableName: String, patterns: List[PatternSpec], bestIdx: Int, epsilon: Double): String = {
    generateBtreeSql(tableName, patterns(bestIdx), epsilon)
  }

  def generateCascadeFirstSpatialSql(tableName: String, probes: PatternProbes): String = {
    generateSpatialIntersectSql(tableName, probes)
  }

  def generateCascadeFirstBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double): String = {
    generateBtreeSql(tableName, pattern, epsilon)
  }
  def generateCascadeNextSpatialSql(tableName: String, probes: PatternProbes, candidateIdsBatch: Array[Long]): String = {
    val baseSql = generateSpatialIntersectSql(tableName, probes)
    addCandidateFilter(baseSql, candidateIdsBatch)
  }
  def generateCascadeNextBtreeSql(tableName: String, pattern: PatternSpec, epsilon: Double, candidateIdsBatch: Array[Long]): String = {
    val baseSql = generateBtreeSql(tableName, pattern, epsilon)
    addCandidateFilter(baseSql, candidateIdsBatch)
  }
  private def addCandidateFilter(sql: String, candidateIds: Array[Long]): String = {
    val idList = candidateIds.mkString(",")

    s"""SELECT time_series_id FROM ($sql) AS base_query
       |WHERE time_series_id = ANY(ARRAY[$idList]::bigint[])""".stripMargin
  }
}
