package org.example.bigdata

import org.example.bigdata.TSseek_03_QueryLanguage._

object TSseek_03_IndexContract {
  val ProbeInfinity = 1.0e15
  private val UnboundedValueMagnitude = 1.0e12
  case class GridSpec(w: Int, h: Double, yMin: Double, yMax: Double, seriesLength: Int) {
    require(w > 0 && h > 0 && yMax > yMin && seriesLength >= 2,
      s"invalid grid w=$w h=$h y=[$yMin, $yMax] length=$seriesLength")
    val xSteps: Int = (seriesLength + w - 1) / w
    val ySteps: Int = math.ceil((yMax - yMin) / h - 1e-9).toInt
    def xBL(i: Int): Double = (i * w).toDouble
    def yBL(j: Int): Double = yMin + j * h
    def cellKey(i: Int, j: Int): String = s"${xBL(i)}_${yBL(j)}_${xBL(i) + w}_${yBL(j) + h}"
    def xTop: Double = xBL(xSteps - 1) + w
    def yTop: Double = yBL(ySteps - 1) + h
    def cellCount: Int = xSteps * ySteps
  }

  private def clampIndex(d: Double, steps: Int): Int =
    if (d.isNaN) 0 else math.max(0.0, math.min((steps - 1).toDouble, d)).toInt
  def forEachSegmentCell(g: GridSpec, x0: Double, y0: Double, x1: Double, y1: Double)(f: (Int, Int) => Unit): Unit = {
    val (ax, ay, bx, by) = if (x0 <= x1) (x0, y0, x1, y1) else (x1, y1, x0, y0)
    val iLo = clampIndex(math.floor(ax / g.w) - 1, g.xSteps)
    val iHi = clampIndex(math.floor(bx / g.w) + 1, g.xSteps)
    var i = iLo
    while (i <= iHi) {
      val cx0 = g.xBL(i)
      val ox0 = math.max(ax, cx0)
      val ox1 = math.min(bx, cx0 + g.w)
      if (ox0 <= ox1) {
        val (ya, yb) =
          if (bx == ax) (ay, by)
          else (ay + (by - ay) * ((ox0 - ax) / (bx - ax)), ay + (by - ay) * ((ox1 - ax) / (bx - ax)))
        val pad = 1e-9 * math.max(1.0, math.max(math.abs(ya), math.abs(yb)))
        val lo = math.min(ya, yb) - pad
        val hi = math.max(ya, yb) + pad
        var j = clampIndex(math.floor((lo - g.yMin) / g.h) - 1, g.ySteps)
        val jHi = clampIndex(math.floor((hi - g.yMin) / g.h) + 1, g.ySteps)
        while (j <= jHi) {
          val c0 = g.yBL(j)
          if (c0 + g.h >= lo && c0 <= hi) f(i, j)
          j += 1
        }
      }
      i += 1
    }
  }
  case class ProbeWindow(x0: Double, x1: Double, y0: Double, y1: Double) {
    def describe: String = s"x=[$x0, $x1] y=[$y0, $y1]"
  }
  def expandLow(bound: Double, epsilon: Double): Double =
    math.max(-ProbeInfinity, bound - epsilon - 1e-9 * math.max(1.0, math.abs(bound)))
  def expandHigh(bound: Double, epsilon: Double): Double =
    math.min(ProbeInfinity, bound + epsilon + 1e-9 * math.max(1.0, math.abs(bound)))
  def probeWindow(p0: Int, p1: Int, d: Domain, epsilon: Double): ProbeWindow =
    ProbeWindow(p0.toDouble, p1.toDouble, expandLow(d.low, epsilon), expandHigh(d.high, epsilon))
  def windowCount(g: GridSpec, w: ProbeWindow, counts: Map[String, Int]): Long = {
    var total = 0L
    var i = clampIndex(math.floor(w.x0 / g.w) - 1, g.xSteps)
    val iHi = clampIndex(math.floor(w.x1 / g.w) + 1, g.xSteps)
    while (i <= iHi) {
      if (g.xBL(i) <= w.x1 && g.xBL(i) + g.w >= w.x0) {
        var j = clampIndex(math.floor((w.y0 - g.yMin) / g.h) - 1, g.ySteps)
        val jHi = clampIndex(math.floor((w.y1 - g.yMin) / g.h) + 1, g.ySteps)
        while (j <= jHi) {
          if (g.yBL(j) <= w.y1 && g.yBL(j) + g.h >= w.y0) total += counts.getOrElse(g.cellKey(i, j), 0)
          j += 1
        }
      }
      i += 1
    }
    total
  }
  def windowInsideGrid(g: GridSpec, w: ProbeWindow): Boolean =
    w.x0 >= 0 && w.x1 <= g.xTop && w.y0 >= g.yMin && w.y1 <= g.yTop
  private def num(d: Double): String = {
    require(!d.isNaN && !d.isInfinite, s"non-finite SQL literal $d")
    val s = java.lang.Double.toString(d)
    if (d < 0) s"($s)" else s
  }
  def probeGeometrySql(w: ProbeWindow): String =
    if (w.x0 == w.x1 && w.y0 == w.y1) s"ST_SetSRID(ST_Point(${num(w.x0)}, ${num(w.y0)}), 4326)"
    else if (w.x0 == w.x1) s"ST_SetSRID(ST_MakeLine(ST_Point(${num(w.x0)}, ${num(w.y0)}), ST_Point(${num(w.x0)}, ${num(w.y1)})), 4326)"
    else s"ST_SetSRID(ST_MakeEnvelope(${num(w.x0)}, ${num(w.y0)}, ${num(w.x1)}, ${num(w.y1)}), 4326)"
  def yBandSql(lo: Double, hi: Double): String = {
    val low = if (lo > -ProbeInfinity) s" AND ST_YMax(time_series_segments) >= ${num(lo)}" else ""
    val high = if (hi < ProbeInfinity) s" AND ST_YMin(time_series_segments) <= ${num(hi)}" else ""
    low + high
  }
  case class SegmentRow(x0: Double, y0: Double, x1: Double, y1: Double, slope: Double,
                        minStep: Option[Double], maxStep: Option[Double], isFluctuating: Boolean) {
    def xMin: Double = math.min(x0, x1)
    def xMax: Double = math.max(x0, x1)
  }
  sealed trait SegmentFilter extends Serializable {
    def sql: String
    def accepts(row: SegmentRow): Boolean
  }
  case class LegacySlopeFilter(direction: String) extends SegmentFilter {
    def sql: String = direction match {
      case Increasing => " AND segment_slope > 0"
      case Decreasing => " AND segment_slope < 0"
      case Flat => " AND segment_slope = 0"
      case _ => ""
    }
    def accepts(r: SegmentRow): Boolean = direction match {
      case Increasing => r.slope > 0
      case Decreasing => r.slope < 0
      case Flat => r.slope == 0
      case _ => true
    }
  }
  case class LegacyFluctuationFilter(direction: String) extends SegmentFilter {
    private def applies = direction == Increasing || direction == Decreasing || direction == Flat
    def sql: String = if (applies) " AND is_fluctuating = false" else ""
    def accepts(r: SegmentRow): Boolean = !applies || !r.isFluctuating
  }
  private def overlapStepsSql(g0: Int, g1: Int): String =
    s"(LEAST(ST_XMax(time_series_segments), $g1) - GREATEST(ST_XMin(time_series_segments), $g0))"

  private def overlapSteps(r: SegmentRow, g0: Int, g1: Int): Double = math.min(r.xMax, g1) - math.max(r.xMin, g0)
  case class SafeSlopeFilter(g0: Int, g1: Int, lower: Option[Double], upper: Option[Double],
                             absUpper: Option[Double], epsilon: Double) extends SegmentFilter {
    private val twoEps = 2 * epsilon
    def sql: String = {
      val k = overlapStepsSql(g0, g1)
      val kDiv = s"GREATEST($k, 1)"
      val parts = lower.map(l => s"segment_slope >= ${num(l)} - ${num(twoEps)} / $kDiv").toList ++
        upper.map(u => s"segment_slope <= ${num(u)} + ${num(twoEps)} / $kDiv") ++
        absUpper.map(a => s"ABS(segment_slope) <= ${num(a)} + ${num(twoEps)} / $kDiv")
      if (parts.isEmpty) "" else s" AND ($k <= 0 OR (${parts.mkString(" AND ")}))"
    }
    def accepts(r: SegmentRow): Boolean = {
      val k = overlapSteps(r, g0, g1)
      k <= 0 || (lower.forall(l => r.slope >= l - twoEps / k) && upper.forall(u => r.slope <= u + twoEps / k) &&
        absUpper.forall(a => math.abs(r.slope) <= a + twoEps / k))
    }
  }

  case class AnchoredStepFilter(g0: Int, g1: Int, lo: Double, hi: Double) extends SegmentFilter {
    def sql: String =
      s" AND (min_step IS NULL OR CASE WHEN ST_XMin(time_series_segments) >= $g0 AND ST_XMax(time_series_segments) <= $g1" +
        s" THEN (min_step >= ${num(lo)} AND max_step <= ${num(hi)})" +
        s" WHEN ${overlapStepsSql(g0, g1)} >= 1 THEN (max_step >= ${num(lo)} AND min_step <= ${num(hi)})" +
        " ELSE TRUE END)"
    def accepts(r: SegmentRow): Boolean = (r.minStep, r.maxStep) match {
      case (Some(mn), Some(mx)) =>
        if (r.xMin >= g0 && r.xMax <= g1) mn >= lo && mx <= hi
        else if (overlapSteps(r, g0, g1) >= 1) mx >= lo && mn <= hi
        else true
      case _ => true
    }
  }
  case class ExistenceStepFilter(lo: Double, hi: Double) extends SegmentFilter {
    def sql: String = s" AND max_step >= ${num(lo)} AND min_step <= ${num(hi)}"
    def accepts(r: SegmentRow): Boolean = (r.minStep, r.maxStep) match {
      case (Some(mn), Some(mx)) => mx >= lo && mn <= hi
      case _ => false
    }
  }
  case class FilterSettings(slopeMode: String, legacyFluctuation: Boolean, stepFilter: Boolean) {
    require(Set("safe", "off", "legacy").contains(slopeMode), s"TSSEEK_SLOPE_FILTER must be safe, off or legacy, got '$slopeMode'")
  }
  object FilterSettings {
    def fromConfig(): FilterSettings =
      FilterSettings(TSseek_Config.SLOPE_FILTER, TSseek_Config.LEGACY_FLUCTUATION_FILTER, TSseek_Config.STEP_FILTER)
  }
  case class StepBounds(lower: Option[Double], upper: Option[Double], absUpper: Option[Double], givesStep: Boolean)

  def stepBounds(direction: String, magnitude: Magnitude, threshold: ThresholdClause, domain: Domain): StepBounds = {
    val maxAbs = {
      val m = math.max(math.abs(domain.low), math.abs(domain.high))
      if (m.isInfinite) UnboundedValueMagnitude else m
    }
    magnitude match {
      case StepMagnitude(lo, _, hi, _, _) =>
        val tau = if (threshold.percent) threshold.value / 100.0 * ((lo + hi) / 2.0) else threshold.value
        val pad = tau + 1e-9 * math.max(1.0, math.max(hi, maxAbs))
        val l = math.max(0.0, lo - pad)
        val u = hi + pad
        direction match {
          case Increasing => StepBounds(Some(l), Some(u), None, givesStep = true)
          case Decreasing => StepBounds(Some(-u), Some(-l), None, givesStep = true)
          case Flat => StepBounds(Some(-u), Some(u), None, givesStep = true)
          case _ => StepBounds(Some(-u), Some(u), Some(u), givesStep = true)
        }
      case _ =>
        direction match {
          case Increasing => StepBounds(Some(0.0), None, None, givesStep = false)
          case Decreasing => StepBounds(None, Some(0.0), None, givesStep = false)
          case Flat if !threshold.percent =>
            val u = threshold.value + 1e-9 * math.max(1.0, maxAbs)
            StepBounds(Some(-u), Some(u), None, givesStep = true)
          case _ => StepBounds(None, None, None, givesStep = false)
        }
    }
  }
  def patternFilters(direction: String, magnitude: Magnitude, threshold: ThresholdClause, domain: Domain,
                     minLength: Int, guaranteed: Option[(Int, Int)], epsilon: Double,
                     settings: FilterSettings, unanchored: Boolean): Vector[SegmentFilter] = {
    val out = Vector.newBuilder[SegmentFilter]
    val bounds = stepBounds(direction, magnitude, threshold, domain)
    val maxAbs = math.min(UnboundedValueMagnitude, math.max(math.abs(domain.low), math.abs(domain.high)))
    val lineError = epsilon + 1e-9 * math.max(1.0, maxAbs)
    settings.slopeMode match {
      case "legacy" => out += LegacySlopeFilter(direction)
      case "safe" =>
        guaranteed.foreach { case (g0, g1) =>
          if (g1 > g0 && (bounds.lower.isDefined || bounds.upper.isDefined || bounds.absUpper.isDefined)) {
            val (lower, upper) = if (bounds.absUpper.isDefined) (None, None) else (bounds.lower, bounds.upper)
            out += SafeSlopeFilter(g0, g1, lower, upper, bounds.absUpper, lineError)
          }
        }
      case _ =>
    }
    if (settings.legacyFluctuation) out += LegacyFluctuationFilter(direction)
    if (settings.stepFilter && bounds.givesStep) {
      val lo = bounds.lower.getOrElse(-ProbeInfinity)
      val hi = bounds.upper.getOrElse(ProbeInfinity)
      guaranteed match {
        case Some((g0, g1)) if g1 > g0 => out += AnchoredStepFilter(g0, g1, lo, hi)
        case None if unanchored && minLength >= 3 => out += ExistenceStepFilter(lo, hi)
        case _ =>
      }
    }
    out.result().filter(_.sql.nonEmpty)
  }
}
