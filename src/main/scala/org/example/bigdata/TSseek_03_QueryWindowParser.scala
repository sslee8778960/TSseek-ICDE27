package org.example.bigdata

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object TSseek_03_QueryWindowParser {

  val DATASET = TSseek_Config.DATASET
  val params = TSseek_Config.getParams()

  val epsilon = params.epsilon
  val defaultLower = params.yMin
  val defaultUpper = params.yMax

  case class SubpatternConstraints(
    valueRange: (Double, Double),
    length: Int,
    lengthRange: Option[(Int, Int)] = None,
    direction: Option[String] = None
  )

  def parseRange(rangeStr: String): (Double, Double) = {
    val trimmed = rangeStr.trim
    if (trimmed.isEmpty) return (defaultLower, defaultUpper)
    val leftBracket = trimmed.head
    val rightBracket = trimmed.last
    val inner = trimmed.substring(1, trimmed.length - 1).trim
    val parts = inner.split(",").map(_.trim)
    val lowerStr = if (parts.length > 0) parts(0) else ""
    val upperStr = if (parts.length > 1) parts(1) else ""
    val lowerParsed = if (lowerStr.isEmpty) defaultLower else lowerStr.toDouble
    val upperParsed = if (upperStr.isEmpty) defaultUpper else upperStr.toDouble
    if (lowerParsed > upperParsed)
      throw new TSseek_03_QueryLanguage.QuerySyntaxException(s"reversed value domain '$rangeStr'; write [low, high]")
    val lower = if (leftBracket == '[') lowerParsed - epsilon else lowerParsed
    val upper = if (rightBracket == ']') upperParsed + epsilon else upperParsed
    (lower, upper)
  }
  def parseDirection(subpattern: String): Option[String] = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    if (cleaned.contains("<+>")) Some("INCREASING")
    else if (cleaned.contains("<->")) Some("DECREASING")
    else if (cleaned.contains("<=>")) Some("FLAT")
    else if (cleaned.contains("<~>")) Some("ANY")
    else None
  }

  def parseLength(subpattern: String): (Int, Option[(Int, Int)], Boolean) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val dynamicLengthRegex = new Regex("\\{(\\d+)\\s*,\\s*(\\d+)\\}")
    dynamicLengthRegex.findFirstMatchIn(cleaned) match {
      case Some(m) =>
        val min = m.group(1).toInt
        val max = m.group(2).toInt
        (min, Some((min, max)), true)
      case None =>
        val fixedLengthRegex = new Regex("\\{(\\d+)\\}")
        val length = fixedLengthRegex.findFirstMatchIn(cleaned).map(_.group(1).toInt).getOrElse(0)
        (length, None, false)
    }
  }
  def parseSubpattern(subpattern: String): SubpatternConstraints = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val (length, lengthRange, isDynamic) = parseLength(subpattern)
    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    val valueRange = rangeRegex.findFirstIn(cleaned).map(parseRange).getOrElse((defaultLower, defaultUpper))
    val direction = parseDirection(subpattern)
    SubpatternConstraints(valueRange, length, lengthRange, direction)
  }
  def parsePattern(pattern: String): List[((Double, Double, Double, Double),
    (Double, Double, Double, Double), String, Option[SubpatternConstraints])] = {
    val trimmedPattern = pattern.trim.stripPrefix("{").stripSuffix("}")
    val subPatternRegex = new Regex("\\|[^|]+\\|")
    val subPatterns = subPatternRegex.findAllIn(trimmedPattern).toList
    val modifiedPattern = subPatternRegex.replaceAllIn(trimmedPattern, "SUBPATTERN")
    val elements = modifiedPattern.split(",(?![^\\[\\]]*\\])").map(_.trim).toList

    var currentIndex = 0
    var subPatternIndex = 0
    var stopParsing = false
    val windows = ListBuffer[((Double, Double, Double, Double), (Double, Double, Double, Double), String, Option[SubpatternConstraints])]()

    val elementsIterator = elements.iterator
    while (elementsIterator.hasNext && !stopParsing) {
      val element = elementsIterator.next()
      element match {
        case e if e == "SUBPATTERN" =>
          val originalSubpattern = subPatterns(subPatternIndex).trim
          val constraints = parseSubpattern(originalSubpattern)
          if (constraints.lengthRange.isDefined) {
            val leftSegment = generateDynamicSubpatternLeftWindow(currentIndex, originalSubpattern)
            val dummyRightSegment = (0.0, 0.0, 0.0, 0.0)
            windows += ((leftSegment, dummyRightSegment, originalSubpattern, Some(constraints)))
            stopParsing = true
          } else {
            val (leftSegment, rightSegment) = generateSubpatternWindow(currentIndex, originalSubpattern, constraints.length)
            windows += ((leftSegment, rightSegment, originalSubpattern, Some(constraints)))
            currentIndex += constraints.length
          }
          subPatternIndex += 1
        case e if e.startsWith("[") || e.startsWith("(") =>
          val window = generateValueRangeWindow(currentIndex, e)
          windows += ((window, window, "nonsubpattern", None))
          currentIndex += 1
        case singleton if singleton.forall(c => c.isDigit || c == '.' || c == '-') =>
          val value = singleton.toDouble
          val window = (currentIndex.toDouble, value, currentIndex.toDouble, value)
          windows += ((window, window, "nonsubpattern", None))
          currentIndex += 1
        case _ =>
      }
    }
    windows.toList
  }
  def parsePatternLegacy(pattern: String): List[((Double, Double, Double, Double),
    (Double, Double, Double, Double), String)] = {
    parsePattern(pattern).map { case (leftWindow, rightWindow, flag, _) =>
      (leftWindow, rightWindow, flag)
    }
  }
  private def generateValueRangeWindow(index: Int, range: String): (Double, Double, Double, Double) = {
    val (lower, upper) = parseRange(range)
    (index.toDouble, lower, index.toDouble, upper)
  }
  private def generateSubpatternWindow(index: Int, subpattern: String, length: Int): ((Double, Double, Double, Double), (Double, Double, Double, Double)) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    rangeRegex.findFirstIn(cleaned) match {
      case Some(fullRangeStr) =>
        val (lower, upper) = parseRange(fullRangeStr)
        val leftSegment = (index.toDouble, lower, index.toDouble, upper)
        val rightSegment = ((index.toDouble + length - 1), lower, (index.toDouble + length - 1), upper)
        (leftSegment, rightSegment)
      case None =>
        ((0.0, 0.0, 0.0, 0.0), (0.0, 0.0, 0.0, 0.0))
    }
  }
  private def generateDynamicSubpatternLeftWindow(index: Int, subpattern: String): (Double, Double, Double, Double) = {
    val cleaned = subpattern.stripPrefix("|").stripSuffix("|").trim
    val rangeRegex = new Regex("\\[[^\\]]+\\]|\\([^\\)]+\\)")
    rangeRegex.findFirstIn(cleaned) match {
      case Some(fullRangeStr) =>
        val (lower, upper) = parseRange(fullRangeStr)
        (index.toDouble, lower, index.toDouble, upper)
      case None =>
        (0.0, 0.0, 0.0, 0.0)
    }
  }

  import TSseek_03_QueryLanguage.{Decreasing, Domain, Increasing, Magnitude, QuerySyntaxException, StepMagnitude,
    ThresholdClause, isNumber, parseDomain, parseMagnitude, parseNonNegativeInt, parseNumber, splitThreshold, splitTopLevel}
  import TSseek_03_IndexContract.{FilterSettings, ProbeWindow, SegmentFilter, patternFilters, probeWindow}
  val Unbounded: Int = Int.MaxValue
  sealed trait WholeConstruct extends Serializable {
    def minLen: Int
    def maxLen: Int
    def text: String
  }
  case class SingletonConstruct(value: Double, text: String) extends WholeConstruct {
    def minLen: Int = 1
    def maxLen: Int = 1
  }
  case class RangeConstruct(domain: Domain, text: String) extends WholeConstruct {
    def minLen: Int = 1
    def maxLen: Int = 1
  }
  case class PatternConstruct(domain: Domain, direction: String, magnitude: Magnitude,
                              minLen: Int, maxLen: Int, text: String) extends WholeConstruct
  case class WildcardConstruct(minLen: Int, maxLen: Int, text: String) extends WholeConstruct
  case class WholeQuery(constructs: Vector[WholeConstruct], threshold: ThresholdClause) {
    def minTotal: Long = constructs.map(_.minLen.toLong).sum
    def maxTotal: Long =
      if (constructs.exists(_.maxLen == Unbounded)) Long.MaxValue else constructs.map(_.maxLen.toLong).sum
    def needsImplicitTail(seriesLength: Int): Boolean = maxTotal < seriesLength

    def resolved(seriesLength: Int): Vector[WholeConstruct] =
      if (needsImplicitTail(seriesLength)) constructs :+ WildcardConstruct(0, Unbounded, "<implicit trailing wildcard>")
      else constructs
  }

  private val WildcardCall = """(?i)^(?:WILDCARD|WC)\s*\((.*)\)$""".r
  private def wildcardLength(arg: String, token: String): (Int, Int) = arg.trim match {
    case "*" | "[*]" => (0, Unbounded)
    case n if n.matches("""\d+""") => (n.toInt, n.toInt)
    case r if r.startsWith("[") && r.endsWith("]") =>
      splitTopLevel(r.substring(1, r.length - 1)).map(_.trim) match {
        case Vector(a, b) =>
          val (lo, hi) = (parseNonNegativeInt(a, s"wildcard '$token'"), parseNonNegativeInt(b, s"wildcard '$token'"))
          if (lo > hi) throw new QuerySyntaxException(s"reversed wildcard length in '$token'")
          (lo, hi)
        case _ => throw new QuerySyntaxException(s"wildcard length must be n, [a, b] or [*] in '$token'")
      }
    case _ => throw new QuerySyntaxException(s"wildcard length must be n, [a, b] or [*] in '$token'")
  }
  def parseWholeQuery(pattern: String): WholeQuery = {
    val (body, threshold) = splitThreshold(pattern)
    if (threshold.percent) throw new QuerySyntaxException(s"whole matching takes an absolute Threshold only: '$pattern'")
    val tokens = splitTopLevel(body)
    val constructs = tokens.map { t =>
      if (t.isEmpty) throw new QuerySyntaxException(s"empty construct (stray comma) in '$pattern'")
      if (t.head == '|') {
        if (t.length < 2 || t.last != '|') throw new QuerySyntaxException(s"unterminated pattern block '$t'")
        val fields = splitTopLevel(t.substring(1, t.length - 1))
        if (fields.length != 4) throw new QuerySyntaxException(s"pattern block needs domain, direction, magnitude, length: '$t'")
        val direction = TSseek_03_QueryLanguage.parseDirection(fields(1))
        val (lo, hi) = TSseek_03_QueryLanguage.parseLength(fields(3))
        PatternConstruct(parseDomain(fields(0)), direction, parseMagnitude(fields(2), direction), lo, hi, t)
      } else if (t == "*") WildcardConstruct(1, 1, t)
      else if (t == "[*]") WildcardConstruct(0, Unbounded, t)
      else if (t.head == '(' && t.drop(1).trim.startsWith("*")) {
        splitTopLevel(t.substring(1, t.length - 1)) match {
          case Vector("*", len) => val (lo, hi) = wildcardLength(len, t); WildcardConstruct(lo, hi, t)
          case _ => throw new QuerySyntaxException(s"wildcard must look like (*, n) or (*, [a, b]): '$t'")
        }
      } else t match {
        case WildcardCall(arg) => val (lo, hi) = wildcardLength(arg, t); WildcardConstruct(lo, hi, t)
        case _ if t.head == '[' || t.head == '(' => RangeConstruct(parseDomain(t), t)
        case _ if isNumber(t) => SingletonConstruct(parseNumber(t, "singleton"), t)
        case _ => throw new QuerySyntaxException(s"unrecognised whole-query construct '$t'")
      }
    }
    if (constructs.isEmpty) throw new QuerySyntaxException(s"empty whole query '$pattern'")
    WholeQuery(constructs, threshold)
  }
  case class Placement(startLo: Int, startHi: Int, minLen: Int, maxLen: Int) {
    def guaranteed: Option[(Int, Int)] = {
      val end = startLo.toLong + minLen - 1
      if (minLen >= 1 && startHi <= end) Some((startHi, end.toInt)) else None
    }
  }
  private def satAdd(a: Long, b: Int): Long = if (b == Unbounded || a == Long.MaxValue) Long.MaxValue else a + b
  def placements(constructs: Vector[WholeConstruct], seriesLength: Int): Option[Vector[Placement]] = {
    val n = constructs.length
    val fwdLo = new Array[Long](n)
    val fwdHi = new Array[Long](n)
    var lo = 0L
    var hi = 0L
    for (i <- 0 until n) {
      fwdLo(i) = lo; fwdHi(i) = hi
      lo += constructs(i).minLen
      hi = satAdd(hi, constructs(i).maxLen)
    }
    if (lo > seriesLength || hi < seriesLength) return None
    val out = new Array[Placement](n)
    var sufMin = 0L
    var sufMax = 0L
    for (i <- (n - 1) to 0 by -1) {
      sufMin += constructs(i).minLen
      sufMax = satAdd(sufMax, constructs(i).maxLen)
      val bwdLo = if (sufMax == Long.MaxValue) Long.MinValue else seriesLength - sufMax
      val bwdHi = seriesLength - sufMin
      val s0 = math.max(fwdLo(i), bwdLo)
      val s1 = math.min(fwdHi(i), bwdHi)
      if (s0 > s1) return None
      out(i) = Placement(s0.toInt, s1.toInt, constructs(i).minLen, constructs(i).maxLen)
    }
    Some(out.toVector)
  }
  case class ConstructProbes(index: Int, construct: WholeConstruct, placement: Placement,
                             windows: Vector[ProbeWindow], filters: Vector[SegmentFilter],
                             extent: Option[(Int, Int)] = None, superset: Boolean = false) {
    def label: String = if (superset) s"superset reading $index" else s"construct $index"
  }
  case class WholeProbePlan(resolved: Vector[WholeConstruct], feasible: Boolean, constructs: Vector[ConstructProbes],
                            supersets: Vector[ConstructProbes] = Vector.empty, infeasibleReason: Option[String] = None,
                            fired: Vector[String] = Vector.empty, lengths: Vector[(Int, Int)] = Vector.empty,
                            rules: String = "backward") {
    def candidates: Vector[ConstructProbes] = constructs ++ supersets
  }

  def planWholeProbes(query: WholeQuery, epsilon: Double, seriesLength: Int, settings: FilterSettings): WholeProbePlan =
    planWholeProbesWithRules(query, epsilon, seriesLength, settings, RewriteRules.Backward, _ => 0L)
  case class RewriteRules(backward: Boolean, tighten: Boolean, superset: Boolean, conflict: Boolean) {
    def name: String = RewriteRules.Allowed.collectFirst { case (k, v) if v == this => k }.getOrElse(toString)
  }
  object RewriteRules {
    val NoRules: RewriteRules = RewriteRules(backward = false, tighten = false, superset = false, conflict = false)
    val Backward: RewriteRules = NoRules.copy(backward = true)
    val All: RewriteRules = RewriteRules(backward = true, tighten = true, superset = true, conflict = true)
    val Allowed: Vector[(String, RewriteRules)] = Vector(
      "none" -> NoRules,
      "backward" -> Backward,
      "backward,tighten" -> Backward.copy(tighten = true),
      "backward,superset" -> Backward.copy(superset = true),
      "backward,conflict" -> Backward.copy(conflict = true),
      "all" -> All)
    def parse(text: String): RewriteRules = {
      val key = text.trim.toLowerCase.replaceAll("\\s+", "")
      Allowed.collectFirst { case (k, v) if k == key => v }.getOrElse(throw new IllegalArgumentException(
        s"TSSEEK_REWRITE_RULES must be one of ${Allowed.map(_._1).mkString(" | ")}, got '$text'"))
    }
    def fromConfig(): RewriteRules = parse(TSseek_Config.REWRITE_RULES)
  }
  def forwardPlacements(constructs: Vector[WholeConstruct], seriesLength: Int): Vector[Option[Placement]] = {
    var lo = 0L
    var hi = 0L
    constructs.map { c =>
      val s0 = lo
      val s1 = math.min(hi, seriesLength.toLong - c.minLen)
      lo += c.minLen
      hi = satAdd(hi, c.maxLen)
      if (s0 > s1) None else Some(Placement(s0.toInt, s1.toInt, c.minLen, c.maxLen))
    }
  }
  def exactExtents(ps: Vector[Placement], seriesLength: Int): Vector[Option[(Int, Int)]] =
    ps.indices.map { i =>
      val minEnd = if (i + 1 < ps.length) ps(i + 1).startLo - 1 else seriesLength - 1
      if (ps(i).startHi <= minEnd) Some((ps(i).startHi, minEnd)) else None
    }.toVector
  def maxEnds(ps: Vector[Placement], seriesLength: Int): Vector[Int] =
    ps.indices.map(i => if (i + 1 < ps.length) ps(i + 1).startHi - 1 else seriesLength - 1).toVector
  def tightenedLengths(constructs: Vector[WholeConstruct], seriesLength: Int): Vector[(Int, Int)] = {
    val sumMin = constructs.map(_.minLen.toLong).sum
    val unbounded = constructs.count(_.maxLen == Unbounded)
    val finiteMax = constructs.filter(_.maxLen != Unbounded).map(_.maxLen.toLong).sum
    constructs.map { c =>
      val othersUnbounded = unbounded - (if (c.maxLen == Unbounded) 1 else 0) > 0
      val othersMax = finiteMax - (if (c.maxLen == Unbounded) 0L else c.maxLen.toLong)
      val lo = if (othersUnbounded) c.minLen.toLong else math.max(c.minLen.toLong, seriesLength - othersMax)
      val hi = math.min(if (c.maxLen == Unbounded) Long.MaxValue else c.maxLen.toLong, seriesLength - (sumMin - c.minLen))
      (lo.toInt, hi.toInt)
    }
  }
  private def lengthTightened(c: WholeConstruct, t: (Int, Int)): Boolean =
    t._1 > c.minLen || (if (c.maxLen == Unbounded) t._2 == 0 else t._2 < c.maxLen)
  def patternContradiction(c: PatternConstruct, minLength: Int, tau: Double): Option[String] =
    stepContradiction(c.direction, c.magnitude, c.domain, minLength, tau)
  def stepContradiction(direction: String, magnitude: Magnitude, domain: Domain, minLength: Int,
                        tau: Double): Option[String] =
    (direction, magnitude) match {
      case (Increasing | Decreasing, StepMagnitude(lo, _, hi, _, _))
          if minLength >= 2 && !domain.low.isInfinite && !domain.high.isInfinite =>
        val big = math.max(math.abs(domain.low), math.abs(domain.high))
        val s = lo - math.max(tau, 1e-9 * math.max(1.0, math.max(math.abs(hi), big)))
        val width = domain.high - domain.low
        val need = (minLength - 1) * s
        if (s > 0 && need > width + 1e-9 * math.max(1.0, big) * minLength)
          Some(s"$minLength readings with steps of at least $s ${if (direction == Increasing) "rise" else "fall"} " +
            s"by at least $need, more than the domain width $width")
        else None
      case _ => None
    }
  def extentProbePositions(g0: Int, g1: Int, countAt: Int => Long): Vector[Int] =
    if (g1 <= g0) Vector(g0)
    else {
      var best = g0
      var bestCount = countAt(g0)
      val endCount = countAt(g1)
      if (endCount < bestCount) { best = g1; bestCount = endCount }
      var r = g0 + 1
      while (r < g1) {
        val n = countAt(r)
        if (n < bestCount) { best = r; bestCount = n }
        r += 1
      }
      val other = if (best == g0) g1 else if (best == g1) g0 else if (best - g0 >= g1 - best) g0 else g1
      Vector(math.min(best, other), math.max(best, other))
    }
  private def constructDomain(c: WholeConstruct): Option[Domain] = c match {
    case s: SingletonConstruct => Some(Domain.point(s.value))
    case r: RangeConstruct => Some(r.domain)
    case p: PatternConstruct => Some(p.domain)
    case _: WildcardConstruct => None
  }
  def supersetCandidates(resolved: Vector[WholeConstruct], ps: Vector[Placement], seriesLength: Int,
                         epsilon: Double): Vector[ConstructProbes] = {
    val maxEnd = maxEnds(ps, seriesLength)
    (0 until seriesLength).flatMap { r =>
      val owners = resolved.indices.filter(i => ps(i).startLo <= r && r <= maxEnd(i))
      val domains = owners.map(i => constructDomain(resolved(i)))
      if (owners.size < 2 || domains.exists(_.isEmpty)) None
      else {
        val low = domains.map(_.get.low).min
        val high = domains.map(_.get.high).max
        if (low.isInfinite || high.isInfinite) None
        else {
          val hull = Domain(low, lowInclusive = true, high, highInclusive = true)
          Some(ConstructProbes(r, RangeConstruct(hull, s"superset of constructs ${owners.mkString(",")}"), Placement(r, r, 1, 1),
            Vector(probeWindow(r, r, hull, epsilon)), Vector.empty, Some((r, r)), superset = true))
        }
      }
    }.toVector
  }
  def planWholeProbesWithRules(query: WholeQuery, epsilon: Double, seriesLength: Int, settings: FilterSettings,
                               rules: RewriteRules, countOf: ProbeWindow => Long): WholeProbePlan = {
    val resolved = query.resolved(seriesLength)
    val written = resolved.map(c => (c.minLen, c.maxLen))
    val forward = forwardPlacements(resolved, seriesLength)
    val fired = Vector.newBuilder[String]
    val starts: Vector[Option[Placement]] =
      if (!rules.backward) forward
      else placements(resolved, seriesLength) match {
        case None =>
          return WholeProbePlan(resolved, feasible = false, Vector.empty,
            infeasibleReason = Some(s"no allocation of construct lengths covers $seriesLength readings"),
            fired = Vector("backward"), lengths = written, rules = rules.name)
        case Some(ps) =>
          val exact = ps.map(p => Some(p): Option[Placement])
          if (exact != forward) fired += "backward"
          exact
      }
    val lengths = if (rules.tighten) tightenedLengths(resolved, seriesLength) else written
    if (rules.tighten && resolved.indices.exists(i => lengthTightened(resolved(i), lengths(i)))) fired += "tighten:lengths"
    val contradiction =
      if (!rules.conflict) None
      else resolved.indices.iterator.flatMap { i =>
        resolved(i) match {
          case c: PatternConstruct =>
            patternContradiction(c, lengths(i)._1, query.threshold.value).map(why => s"construct $i '${c.text.take(60)}': $why")
          case _ => None
        }
      }.nextOption()
    contradiction match {
      case Some(reason) =>
        return WholeProbePlan(resolved, feasible = false, Vector.empty, infeasibleReason = Some(s"contradiction in $reason"),
          fired = fired.result() :+ "conflict", lengths = lengths, rules = rules.name)
      case None =>
    }
    val extents: Vector[Option[(Int, Int)]] =
      if (rules.tighten) exactExtents(starts.map(_.get), seriesLength)
      else starts.map(_.flatMap(_.guaranteed))
    if (rules.tighten && resolved.indices.exists(i => constructDomain(resolved(i)).isDefined && extents(i) != starts(i).flatMap(_.guaranteed)))
      fired += "tighten:extent"
    var moved = false
    def windows(i: Int, d: Domain, p: Placement): Vector[ProbeWindow] = extents(i) match {
      case Some((g0, g1)) =>
        val ends = if (g1 > g0) Vector(g0, g1) else Vector(g0)
        val at = if (rules.tighten) extentProbePositions(g0, g1, r => countOf(probeWindow(r, r, d, epsilon))) else ends
        if (at != ends) moved = true
        at.map(r => probeWindow(r, r, d, epsilon))
      case None => Vector(probeWindow(p.startLo, p.startHi, d, epsilon))
    }
    val probes = resolved.indices.flatMap { i =>
      starts(i).flatMap { p =>
        resolved(i) match {
          case c: SingletonConstruct => Some(ConstructProbes(i, c, p, windows(i, Domain.point(c.value), p), Vector.empty, extents(i)))
          case c: RangeConstruct => Some(ConstructProbes(i, c, p, windows(i, c.domain, p), Vector.empty, extents(i)))
          case c: PatternConstruct =>
            val filters = patternFilters(c.direction, c.magnitude, query.threshold, c.domain, c.minLen,
              extents(i), epsilon, settings, unanchored = false)
            Some(ConstructProbes(i, c, p, windows(i, c.domain, p), filters, extents(i)))
          case _: WildcardConstruct => None
        }
      }
    }.toVector
    if (moved) fired += "tighten:positions"
    val supersets = if (rules.superset) supersetCandidates(resolved, starts.map(_.get), seriesLength, epsilon) else Vector.empty
    if (supersets.nonEmpty) fired += s"superset:${supersets.size}"

    WholeProbePlan(resolved, feasible = true, probes, supersets, None, fired.result(), lengths, rules.name)
  }
  case class WholeSelection(counts: Vector[(ConstructProbes, Vector[Long])], chosen: Option[(ConstructProbes, Long)])
  def selectWholeCandidate(plan: WholeProbePlan, countOf: ProbeWindow => Long): WholeSelection = {
    val counts = plan.candidates.map(c => (c, c.windows.map(countOf)))
    val chosen = if (counts.isEmpty) None else Some(counts.map { case (c, ns) => (c, ns.min) }.minBy(_._2))
    WholeSelection(counts, chosen)
  }
  def constructLogLine(c: ConstructProbes, counts: Vector[Long]): String = {
    val p = c.placement
    s"!!!Construct ${c.index} '${c.construct.text.take(60)}' start=[${p.startLo},${p.startHi}] " +
      s"guaranteed=${c.extent.getOrElse("none")} windows=${c.windows.map(_.describe).mkString("; ")} " +
      s"counts=${counts.mkString(",")} filters=${c.filters.size}"
  }
  def supersetLogLine(counts: Vector[(ConstructProbes, Vector[Long])]): String = {
    val runs = counts.foldLeft(Vector.empty[(Int, Int, ProbeWindow)]) { case (acc, (c, _)) =>
      val w = c.windows.head
      acc.lastOption match {
        case Some((a, b, v)) if b == c.index - 1 && v.y0 == w.y0 && v.y1 == w.y1 => acc.init :+ ((a, c.index, v))
        case _ => acc :+ ((c.index, c.index, w))
      }
    }
    s"!!!Superset candidates: ${counts.size} (reading:count) " + counts.map { case (c, ns) => s"${c.index}:${ns.min}" }.mkString(" ") +
      " hulls: " + runs.map { case (a, b, w) => s"[$a,$b] y=[${w.y0}, ${w.y1}]" }.mkString("; ")
  }
  def chosenLogLine(c: ConstructProbes, estimate: Long): String =
    s"Chosen ${c.label} (estimate $estimate): ${c.windows.size} probe(s), INTERSECT, ${c.filters.size} segment filter(s)"
}
