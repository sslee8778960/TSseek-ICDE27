package org.example.bigdata

object TSseek_03_QueryLanguage {
  class QuerySyntaxException(message: String) extends IllegalArgumentException(message)
  private def fail(message: String): Nothing = throw new QuerySyntaxException(message)
  private val NumberText = """[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"""
  def isNumber(text: String): Boolean = text.trim.matches(NumberText)
  def parseNumber(text: String, what: String): Double = {
    val t = text.trim
    if (!t.matches(NumberText)) fail(s"$what: expected a number, got '$text'")
    t.toDouble
  }
  def parseNonNegativeInt(text: String, what: String): Int = {
    val t = text.trim
    if (!t.matches("""\d+""")) fail(s"$what: expected a non-negative integer, got '$text'")
    t.toInt
  }
  def stepTolerance(declared: Double, step: Double, prev: Double, curr: Double): Double =
    math.max(declared, 1e-9 * math.max(1.0, math.max(math.abs(step), math.max(math.abs(prev), math.abs(curr)))))

  case class Domain(low: Double, lowInclusive: Boolean, high: Double, highInclusive: Boolean) {
    def contains(v: Double): Boolean =
      (if (lowInclusive) v >= low else v > low) && (if (highInclusive) v <= high else v < high)
  }
  object Domain {
    def point(v: Double): Domain = Domain(v, lowInclusive = true, v, highInclusive = true)
  }

  def parseDomain(text: String): Domain = {
    val t = text.trim
    if (t.length < 2 || !"[(".contains(t.head) || !"])".contains(t.last))
      fail(s"value domain must be bracketed like [a, b] or (a, b], got '$text'")
    val parts = splitTopLevel(t.substring(1, t.length - 1))
    if (parts.length != 2) fail(s"value domain must have exactly two bounds, got '$text'")
    def bound(p: String): Option[Double] =
      if (p.isEmpty || p == "*") None else Some(parseNumber(p, s"bound of domain '$text'"))
    val low = bound(parts(0))
    val high = bound(parts(1))
    val d = Domain(
      low.getOrElse(Double.NegativeInfinity), low.isDefined && t.head == '[',
      high.getOrElse(Double.PositiveInfinity), high.isDefined && t.last == ']')
    if (d.low > d.high) fail(s"reversed value domain '$text': lower bound ${d.low} exceeds upper bound ${d.high}; write [low, high]")
    d
  }
  val Increasing = "INCREASING"
  val Decreasing = "DECREASING"
  val Flat = "FLAT"
  val AnyDirection = "ANY"
  def parseDirection(text: String): String = text.trim match {
    case "<+>" => Increasing
    case "<->" => Decreasing
    case "<=>" => Flat
    case "<~>" | "<*>" | "<?>" => AnyDirection
    case other => fail(s"unknown direction '$other' (expected <+>, <->, <=>, <~>)")
  }

  sealed trait Magnitude extends Serializable
  case object SignOnly extends Magnitude
  case class StepMagnitude(lo: Double, loInclusive: Boolean, hi: Double, hiInclusive: Boolean, scalar: Boolean) extends Magnitude
  case class FactorMagnitude(multiplier: Double) extends Magnitude

  def parseMagnitude(text: String, direction: String): Magnitude = {
    val t = text.trim
    if (t.isEmpty || t == "**" || t == "*") return SignOnly
    if (t.length > 1 && (t.endsWith("x") || t.endsWith("X")) && isNumber(t.dropRight(1)))
      return FactorMagnitude(parseNumber(t.dropRight(1), "factor magnitude"))
    val (lo, loInc, hi, hiInc, scalar) =
      if (t.length > 2 && t.head == '*' && t.last == '*') {
        val m = parseNumber(t.substring(1, t.length - 1), "magnitude")
        (m, true, m, true, true)
      } else if (isNumber(t)) {
        val m = parseNumber(t, "magnitude")
        (m, true, m, true, true)
      } else if ("[(".contains(t.head)) {
        val d = parseDomain(t)
        if (d.low.isInfinite || d.high.isInfinite) fail(s"magnitude interval '$text' must be bounded")
        (d.low, d.lowInclusive, d.high, d.highInclusive, false)
      } else fail(s"cannot parse magnitude '$text'")
    val unsigned =
      if (lo >= 0) StepMagnitude(lo, loInc, hi, hiInc, scalar)
      else if (hi <= 0) {
        if (direction == Increasing)
          fail(s"negative magnitude '$text' contradicts <+>; magnitudes are unsigned and the direction carries the sign")
        StepMagnitude(-hi, hiInc, -lo, loInc, scalar)
      } else fail(s"mixed-sign magnitude interval '$text'; magnitudes are unsigned and the direction carries the sign")
    if (direction == Flat && unsigned.hi != 0.0)
      fail(s"<=> with non-zero magnitude '$text' is contradictory; use ** and Threshold:")
    unsigned
  }
  def stepSatisfies(direction: String, magnitude: Magnitude, tau: Double, prev: Double, curr: Double): Boolean = {
    val delta = curr - prev
    direction match {
      case Increasing => if (!(delta > 0)) return false
      case Decreasing => if (!(delta < 0)) return false
      case Flat => if (math.abs(delta) > stepTolerance(tau, 0.0, prev, curr)) return false
      case _ =>
    }
    magnitude match {
      case SignOnly => true
      case StepMagnitude(lo, loInc, hi, hiInc, _) =>
        val a = math.abs(delta)
        val slack = stepTolerance(0.0, hi, prev, curr)
        val lowerOk = if (loInc) a >= lo - math.max(tau, slack) else a > lo - tau + slack
        val upperOk = if (hiInc) a <= hi + math.max(tau, slack) else a < hi + tau - slack
        lowerOk && upperOk
      case FactorMagnitude(f) =>
        val expected = prev * f
        math.abs(curr - expected) <= stepTolerance(tau, expected, prev, curr)
    }
  }
  def parseLength(text: String, minimum: Int = 1): (Int, Int) = {
    val t = text.trim
    if (t.length < 2 || t.head != '{' || t.last != '}') fail(s"length must look like {n} or {min, max}, got '$text'")
    val parts = t.substring(1, t.length - 1).split(",", -1).map(_.trim)
    val (a, b) = parts match {
      case Array(n) => val v = parseNonNegativeInt(n, s"length '$text'"); (v, v)
      case Array(x, y) => (parseNonNegativeInt(x, s"length '$text'"), parseNonNegativeInt(y, s"length '$text'"))
      case _ => fail(s"length must look like {n} or {min, max}, got '$text'")
    }
    if (a > b) fail(s"reversed length '$text': min $a exceeds max $b")
    if (a < minimum) fail(s"length '$text' below the minimum $minimum")
    (a, b)
  }
  case class ThresholdClause(value: Double, percent: Boolean)
  private val ThresholdRe = ("""(?i)\bThreshold\s*:\s*(""" + NumberText + """)\s*(%?)\s*$""").r
  def splitThreshold(query: String): (String, ThresholdClause) = {
    ThresholdRe.findFirstMatchIn(query) match {
      case Some(m) =>
        val v = m.group(1).toDouble
        if (v < 0) fail(s"negative Threshold in '$query'")
        (query.substring(0, m.start).trim, ThresholdClause(v, m.group(2) == "%"))
      case None =>
        if (query.toLowerCase.contains("threshold")) fail(s"malformed Threshold clause in '$query' (expected a trailing 'Threshold: <number>[%]')")
        (query.trim, ThresholdClause(0.0, percent = false))
    }
  }
  def splitTopLevel(text: String): Vector[String] = {
    val out = Vector.newBuilder[String]
    val current = new StringBuilder
    var depth = 0
    var inBlock = false
    text.foreach { c =>
      c match {
        case '|' => inBlock = !inBlock; current += c
        case '[' | '(' | '{' if !inBlock => depth += 1; current += c
        case ']' | ')' | '}' if !inBlock => depth -= 1; current += c
        case ',' if depth == 0 && !inBlock => out += current.toString.trim; current.clear()
        case _ => current += c
      }
    }
    if (inBlock) fail(s"unterminated | ... | block in '$text'")
    if (depth != 0) fail(s"unbalanced brackets in '$text'")
    out += current.toString.trim
    out.result()
  }
  sealed trait Anchor extends Serializable
  case class AtPosition(position: Int) extends Anchor
  case class AtRange(first: Int, last: Int) extends Anchor
  sealed trait Separator extends Serializable
  case object AdjacentSeparator extends Separator
  case class GapSeparator(min: Int, max: Int) extends Separator
  case object FlexSeparator extends Separator
  case class LexedBlock(fields: Vector[String], anchor: Option[Anchor])
  case class LexedSubsequence(
    unordered: Boolean,
    blocks: Vector[LexedBlock],
    separators: Vector[Separator],
    threshold: ThresholdClause
  )
  def lexSubsequence(query: String): LexedSubsequence = {
    val unorderedRe = """(?i)^\s*UNORDERED\s*:\s*""".r
    val unordered = unorderedRe.findFirstIn(query).isDefined
    val (body, threshold) = splitThreshold(unorderedRe.replaceFirstIn(query, ""))
    val s = body
    var i = 0
    def skipWs(): Unit = while (i < s.length && s.charAt(i).isWhitespace) i += 1
    def expect(c: Char, what: String): Unit = {
      skipWs()
      if (i >= s.length || s.charAt(i) != c)
        fail(s"expected '$c' ($what) at position $i of '$query', found '${s.substring(math.min(i, s.length))}'")
      i += 1
    }
    def readInt(what: String): Int = {
      skipWs()
      val start = i
      while (i < s.length && s.charAt(i).isDigit) i += 1
      if (start == i) fail(s"expected an integer ($what) at position $start of '$query'")
      s.substring(start, i).toInt
    }
    val blocks = Vector.newBuilder[LexedBlock]
    val seps = Vector.newBuilder[Separator]
    var more = true
    while (more) {
      expect('|', "start of pattern block")
      val close = s.indexOf('|', i)
      if (close < 0) fail(s"unterminated pattern block in '$query'")
      val fields = splitTopLevel(s.substring(i, close))
      if (fields.length != 4) fail(s"pattern block must have 4 fields (domain, direction, magnitude, length), got ${fields.length}: '|${s.substring(i, close)}|'")
      i = close + 1
      skipWs()
      val anchor =
        if (i < s.length && s.charAt(i) == '@') {
          i += 1
          skipWs()
          if (i < s.length && s.charAt(i) == '[') {
            i += 1
            val a = readInt("anchor range start")
            expect(',', "anchor range")
            val b = readInt("anchor range end")
            expect(']', "anchor range end")
            if (a > b) fail(s"reversed anchor range @[$a, $b] in '$query'")
            Some(AtRange(a, b))
          } else Some(AtPosition(readInt("anchor position")))
        } else None
      blocks += LexedBlock(fields, anchor)
      skipWs()
      if (i >= s.length) more = false
      else {
        expect(',', "separator between pattern blocks")
        skipWs()
        if (i < s.length && s.charAt(i) == '#') {
          i += 1
          val a = readInt("gap length")
          skipWs()
          val b = if (i < s.length && s.charAt(i) == '-') { i += 1; readInt("gap upper length") } else a
          expect('#', "end of gap")
          if (a > b) fail(s"reversed gap #$a-$b# in '$query'")
          expect(',', "comma after gap")
          seps += GapSeparator(a, b)
        } else if (s.startsWith("....", i)) {
          i += 4
          expect(',', "comma after ....")
          seps += FlexSeparator
        } else seps += AdjacentSeparator
      }
    }
    val lexed = LexedSubsequence(unordered, blocks.result(), seps.result(), threshold)
    if (unordered && !lexed.separators.forall(_ == FlexSeparator))
      fail(s"UNORDERED: requires '....' between every pair of patterns in '$query'")
    lexed
  }
}
