package org.example.bigdata

/**
 * Configuration File
 *
 * Every setting is read from a TSSEEK_* environment variable on the Spark driver (see README).
 */
object TSseek_Config {
  private def envOr(key: String, default: String): String = {
    val v = System.getenv(key)
    if (v == null || v.trim.isEmpty) default else v.trim
  }
  private def requiredEnv(key: String): String = {
    val value = System.getenv(key)
    require(value != null && value.trim.nonEmpty, s"Required configuration is missing: $key")
    value.trim
  }
  val DATASET: String        = envOr("TSSEEK_DATASET",       "ECG")   // Options: "TSBS", "ECG", "RANDOMWALK", "REFIT"
  val DATASET_SIZE: String   = envOr("TSSEEK_DATASET_SIZE",  "25m")   // Options: "25m", "50m", "100m", "200m"
  val ALPHA: Double          = envOr("TSSEEK_ALPHA",         "0.6").toDouble   // Segmentation threshold multiplier
  val KEEP_SEGMENT_TABLES: Boolean = envOr("TSSEEK_KEEP_SEGMENT_TABLES", "false").toBoolean
  val MATCHING_TYPE: String  = envOr("TSSEEK_MATCHING_TYPE", "SUBSEQUENCE")   // Options: "WHOLE_SEQUENCE", "SUBSEQUENCE"

  // Subsequence query case, used when MATCHING_TYPE = "SUBSEQUENCE".
  // Single-pattern cases:
  //   "FREE_FIXED_LEN", "FREE_DYN_LEN", "BOUNDED_FIXED_LEN", "BOUNDED_DYN_LEN",
  //   "FIXED_START_FIXED_LEN", "FIXED_START_DYN_LEN"
  // Multi-pattern cases:
  //   "MULTI_ADJACENT", "MULTI_FIXED_GAP", "MULTI_RANGE_GAP",
  //   "MULTI_ORDERED_FLEX_GAP", "MULTI_UNORDERED_FLEX_GAP"
  val SUBSEQUENCE_CASE: String       = envOr("TSSEEK_SUBSEQUENCE_CASE",       "FREE_FIXED_LEN")
  val MULTI_PATTERN_STRATEGY: String = envOr("TSSEEK_MULTI_PATTERN_STRATEGY", "CASCADE")   // Options: "COMPOSITE", "INTERSECT_ALL", "BEST_ONLY", "CASCADE"
  val LEGACY_FLUCTUATION_FILTER: Boolean = envOr("TSSEEK_LEGACY_FLUCTUATION_FILTER", "false").toBoolean
  val USE_FLUCTUATION_FILTER: Boolean = LEGACY_FLUCTUATION_FILTER
  if (System.getenv("TSSEEK_USE_FLUCTUATION_FILTER") != null)
    println("!!! TSSEEK_USE_FLUCTUATION_FILTER is ignored; set TSSEEK_LEGACY_FLUCTUATION_FILTER=true to enable the fluctuation filter")
  val SLOPE_FILTER: String = envOr("TSSEEK_SLOPE_FILTER", "safe").toLowerCase
  require(Set("safe", "off", "legacy").contains(SLOPE_FILTER), s"TSSEEK_SLOPE_FILTER must be safe, off or legacy, got '$SLOPE_FILTER'")
  val STEP_FILTER: Boolean = envOr("TSSEEK_STEP_FILTER", "false").toBoolean
  val REWRITE_RULES: String = envOr("TSSEEK_REWRITE_RULES", "backward").toLowerCase.replaceAll("\\s+", "")
  require(Set("none", "backward", "backward,tighten", "backward,superset", "backward,conflict", "all").contains(REWRITE_RULES),
    s"TSSEEK_REWRITE_RULES must be none, backward, backward,tighten, backward,superset, backward,conflict or all, got '$REWRITE_RULES'")

  val SERIES_LENGTH: Int = envOr("TSSEEK_SERIES_LENGTH", "128").toInt
  require(SERIES_LENGTH >= 2, s"TSSEEK_SERIES_LENGTH must be >= 2, got $SERIES_LENGTH")
  def numTables(size: String = DATASET_SIZE): Int = size match {
    case "25m" => 40
    case "50m" => 80
    case "100m" => 160
    case "200m" => 320
    case _ => throw new IllegalArgumentException(s"Unknown dataset size: $size")
  }
  def datasetPrefix(dataset: String = DATASET, size: String = DATASET_SIZE, length: Int = SERIES_LENGTH): String =
    s"${dataset.toLowerCase}_$size${if (length == 128) "" else s"_len$length"}"
  def segmentsTablePrefix: String = s"${datasetPrefix()}_segments_table"
  def rawTablePrefix: String = s"${datasetPrefix()}_og_ts"
  def mappingHdfsBase: String = s"hdfs://shark1local:9000/user/YOUR_USERNAME/output/${datasetPrefix()}_mapping"
  def segmentsLocalDir: String = s"/home/YOUR_USERNAME/results/${datasetPrefix()}_segments/"

  def ecgInput(size: String = DATASET_SIZE, length: Int = SERIES_LENGTH): (String, Int => String) = {
    val sizeTag = size match {
      case "25m" => "025M"; case "50m" => "050M"; case "100m" => "100M"; case "200m" => "200M"
      case _ => throw new IllegalArgumentException(s"Unknown ECG size: $size")
    }
    val dir = size match {
      case "25m" => s"ecg_025m_40_files_len$length"
      case "50m" => s"ecg_050m_80_files_len$length"
      case "100m" => s"ecg_100m_160_files_len$length"
      case _ if length == 128 => "ecg_200m_320_files"
      case _ => throw new IllegalArgumentException(s"No ECG $size input at length $length")
    }
    val fileName = (i: Int) => if (length == 128) s"time_series_${sizeTag}_$i.csv" else s"time_series_${sizeTag}_len${length}_$i.csv"
    val overrideDir = envOr("TSSEEK_ECG_INPUT_HDFS", "").stripSuffix("/")
    (if (overrideDir.nonEmpty) overrideDir else s"hdfs://shark1local:9000/user/YOUR_USERNAME/input/ecg/$dir", fileName)
  }
  val WHOLE_CASE: String = envOr("TSSEEK_WHOLE_CASE", "")

  val OUTPUT_POSITIONS: Boolean = envOr("TSSEEK_OUTPUT_POSITIONS", "false").toBoolean
  val SAVE_CANDIDATES: Boolean = envOr("TSSEEK_SAVE_CANDIDATES", "false").toBoolean
  val TASKS_PER_CORE: Int = envOr("TSSEEK_TASKS_PER_CORE", "3").toInt
  val MIN_CHUNK: Int      = envOr("TSSEEK_MIN_CHUNK", "2000").toInt
  def refitInputHdfs: String = requiredEnv("TSSEEK_REFIT_INPUT_HDFS").stripSuffix("/")
  def refitInputFileName(number: Int): String = {
    val template = requiredEnv("TSSEEK_REFIT_FILE_TEMPLATE")
    require(template.contains("%"),
      s"TSSEEK_REFIT_FILE_TEMPLATE must contain a format specifier such as %d (got '$template'); otherwise every table reads the same file")
    String.format(java.util.Locale.ROOT, template, Int.box(number))
  }
  val TSBS_PATTERN = "| [12, 42], <+>, 0.95, {32} |, | [12, 42], <->, 1.00, {32} |, | [10, 38], <+>, 0.25, {24, 25} |, | [12, 38], <->, 0.30, {39, 40} |"
  val ECG_PATTERN = "| [-0.45, 0.85], <+>, 0.03, {32} |, | [-0.45, 0.85], <->, 0.04, {32} |, | [-0.20, 1.10], <+>, 0.02, {24, 25} |, | [-0.60, 1.10], <->, 0.03, {39, 40} |"
  val RANDOMWALK_PATTERN = "| [-4.5, -2.5], <+>, *0.01*, {32} |, | [-0.6, 1.8], <->, *0.02*, {32} |, | [-2.0, 2.0], <+>, *0.01*, {24, 25} |, | [2.0, 4.0], <->, *0.02*, {39, 40} |"

  val TSBS_SUBSEQUENCE_PATTERN = "| [12, 42], <+>, 0.95, {32} | Threshold: 0"

  val ECG_SUBSEQUENCE_PATTERN = "| [-0.15, 0.05], <=>, **, {29} | Threshold: 0"
  val RANDOMWALK_SUBSEQUENCE_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} | Threshold: 0"

  val ECG_FREE_FIXED_LEN         = "| [-0.30, -0.18], <+>, **, {5} | Threshold: 0"
  val ECG_FREE_DYN_LEN           = "| [-0.30, -0.18], <+>, **, {4, 6} | Threshold: 0"
  val ECG_FREE_DYN_LEN_WIDE      = "| [-0.30, -0.18], <+>, **, {4, 9} | Threshold: 0"
  val ECG_BOUNDED_FIXED_LEN      = "| [-0.30, -0.18], <+>, **, {5} | @[0, 31] Threshold: 0"
  val ECG_BOUNDED_DYN_LEN        = "| [-0.30, -0.18], <+>, **, {4, 6} | @[0, 31] Threshold: 0"
  val ECG_FIXED_START_FIXED_LEN  = "| [-0.30, -0.18], <+>, **, {5} | @21 Threshold: 0"
  val ECG_FIXED_START_DYN_LEN    = "| [-0.30, -0.18], <+>, **, {4, 6} | @21 Threshold: 0"
  val ECG_MULTI_ADJACENT         = "| [-0.35, -0.18], <+>, **, {5} |, #0#, | [-0.35, -0.18], <->, **, {7} | Threshold: 0"
  val ECG_MULTI_FIXED_GAP        = "| [-0.35, -0.18], <+>, **, {5} |, #5#, | [-0.35, -0.18], <->, **, {7} | Threshold: 0"
  val ECG_MULTI_RANGE_GAP        = "| [-0.35, -0.18], <+>, **, {5} |, #3-8#, | [-0.35, -0.18], <->, **, {7} | Threshold: 0"
  val ECG_MULTI_ORDERED_FLEX     = "| [-0.35, -0.18], <+>, **, {5} |, ...., | [-0.35, -0.18], <->, **, {7} | Threshold: 0"
  val ECG_MULTI_UNORDERED_FLEX   = "UNORDERED: | [-0.35, -0.18], <+>, **, {5} |, ...., | [-0.35, -0.18], <->, **, {7} | Threshold: 0"

  val ECG_SUBSEQUENCE_1A_PATTERN = ECG_FIXED_START_FIXED_LEN
  val ECG_SUBSEQUENCE_1B_PATTERN = ECG_BOUNDED_FIXED_LEN
  val ECG_SUBSEQUENCE_2A_PATTERN = "| [-0.35, -0.18], <+>, **, {5} | @10, | [-0.35, -0.18], <->, **, {7} | @20 Threshold: 0"
  val ECG_SUBSEQUENCE_2B_PATTERN = ECG_MULTI_FIXED_GAP
  val ECG_SUBSEQUENCE_2C_PATTERN = ECG_MULTI_ORDERED_FLEX
  val TSBS_SUBSEQUENCE_1A_PATTERN = "| [12, 42], <+>, 0.95, {32} | @10 Threshold: 0"
  val TSBS_SUBSEQUENCE_1B_PATTERN = "| [12, 42], <+>, 0.95, {32} | @[10, 80] Threshold: 0"
  val TSBS_SUBSEQUENCE_2A_PATTERN = "| [12, 42], <+>, 0.95, {32} | @0, | [12, 42], <->, 1.00, {32} | @60 Threshold: 0"
  val TSBS_SUBSEQUENCE_2B_PATTERN = "| [12, 42], <+>, 0.95, {32} |, #20#, | [12, 42], <->, 1.00, {32} | @[0, 100] Threshold: 0"
  val TSBS_SUBSEQUENCE_2C_PATTERN = "| [12, 42], <+>, 0.95, {32} |, ...., | [12, 42], <->, 1.00, {32} | Threshold: 0"

  val RANDOMWALK_SUBSEQUENCE_1A_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} | @10 Threshold: 0"
  val RANDOMWALK_SUBSEQUENCE_1B_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} | @[10, 80] Threshold: 0"
  val RANDOMWALK_SUBSEQUENCE_2A_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} | @0, | [-0.6, 1.8], <->, 0.02, {32} | @60 Threshold: 0"
  val RANDOMWALK_SUBSEQUENCE_2B_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} |, #20#, | [-0.6, 1.8], <->, 0.02, {32} | @[0, 100] Threshold: 0"
  val RANDOMWALK_SUBSEQUENCE_2C_PATTERN = "| [-4.5, -2.5], <+>, 0.01, {32} |, ...., | [-0.6, 1.8], <->, 0.02, {32} | Threshold: 0"

  val REFIT_WHOLE_T1 = "654, 659, 659, [605, 739], [616, 752], [620, 756], | [666, 731], <+>, **, {3} |, 725, 731, 745, [676, 826], [688, 840], [707, 863], | [787, 865], <+>, **, {3} |, 845, 857, 854, [769, 939], [765, 933], [755, 921], | [791, 856], <->, **, {3} |, 810, 796, 788, [710, 866], [684, 834], [670, 818], | [682, 759], <->, **, {3} |, 708, 705, 703, [630, 770], [647, 789], [658, 804], | [734, 860], <+>, **, {3} |, 1406, 1078, 924, [789, 963], [767, 937], [753, 919], | [728, 833], <->, **, {3} |, 753, 739, 749, [702, 856], [702, 856], [796, 972], | [829, 937], <+>, **, {3} |, 1015, 1502, 1732, [1525, 1863], [1472, 1798], [1044, 1276], | [1116, 1753], <~>, **, {3} |, 950, 1126, 984, [945, 1155], [1364, 1666], [1322, 1614], | [876, 1735], <+>, **, {3} |, 1710, 1706, 1742, [1103, 1347], [892, 1090], [982, 1200], | [1167, 1948], <+>, **, {3} |, 1896, 1935, 1947, [1753, 2141], [1607, 1963], [1607, 1963], | [1201, 1738], <->, **, {3} |, 832, 787, 787, [626, 764], [621, 757], [624, 762], | [680, 756], <+>, **, {3} |, 763, 799, 880, [1156, 1412], [828, 1012], [792, 966], | [813, 887], <->, **, {3} |, 818, 850, 977, [851, 1039], [839, 1025], [862, 1052], | [1056, 1787], <->, **, {3} |, 1151, [1639, 2003]"
  val REFIT_WHOLE_T2 = "387, 179, 308, 219, 196, 169, 178, 243, [332, 404], [404, 492], [455, 555], [522, 638], [561, 685], [578, 706], [616, 752], [645, 787], | [736, 1054], <+>, **, {8} |, | [138, 1061], <~>, **, {7, 9} |, 181, 180, 188, 199, 200, 200, 193, 188, [163, 213], [149, 199], [140, 190], [140, 190], [138, 188], [114, 164], [120, 170], [121, 171], | [128, 219], <+>, **, {8} |, | [149, 215], <~>, **, {7, 9} |, 167, 160, 159, 163, 153, 153, 151, 151, [127, 177], [120, 170], [120, 170], [113, 163], [1005, 1227], [1008, 1232], [1008, 1232], [996, 1216], | [893, 1135], <~>, **, {8} |, | [805, 905], <->, **, {7, 9} |, 844, 820, 797, 794, 766, 754, 753, 772, [720, 878], [779, 951], [796, 972], [802, 980], [813, 993], [810, 988], [812, 992], [807, 985], | [179, 960], <~>, **, {8} |, | [123, 224], <->, **, {7, 9} |"
  val REFIT_WHOLE_T3 = "[673, 821], [721, 881], [753, 919], [786, 960], [796, 972], [797, 973], [792, 968], | [542, 894], <->, **, {7} |, | [459, 575], <~>, **, {6, 8} |, [437, 533], [444, 542], [444, 542], [447, 545], [453, 553], [460, 562], [463, 565], | [493, 668], <+>, **, {7} |, | [618, 1049], <+>, **, {6, 8} |, [971, 1185], [971, 1185], [1009, 1233], [1044, 1276], [1046, 1278], [1055, 1289], [1059, 1293], | [1169, 1302], <~>, **, {7} |, | [1108, 1267], <~>, **, {6, 8} |, [968, 1182], [869, 1061], [834, 1018], [804, 982], [776, 948], [751, 917], [724, 884], | [768, 920], <~>, **, {7} |, | [913, 1248], <~>, **, {6, 8} |, [1110, 1356], [1095, 1337], [1152, 1408], [1165, 1423], [1205, 1471], [1153, 1409], [1135, 1387], | [989, 1248], <->, **, {7} |, | [828, 1039], <->, **, {6, 8} |, [812, 992], [806, 984], [788, 962], [769, 939], [769, 939], [769, 939], [746, 910], [743, 907], | [725, 847], <->, **, {8} |, | [599, 772], <~>, **, {6, 8} |"
  val REFIT_WHOLE_T3L = "[789, 963], [792, 968], [792, 968], [795, 971], [801, 977], [804, 982], [810, 988], [819, 1001], [813, 993], [815, 995], [819, 999], [819, 999], [831, 1015], [828, 1010], [838, 1024], [840, 1026], [828, 1010], [158, 208], [160, 210], [160, 210], [164, 214], [172, 222], | [179, 255], <~>, **, {22} |, | [192, 686], <+>, **, {19, 23} |, [597, 729], [606, 740], [614, 750], [613, 749], [612, 748], [618, 754], [618, 754], [625, 763], [612, 748], [616, 752], [594, 726], [606, 740], [588, 718], [561, 685], [567, 691], [548, 668], [526, 642], [534, 652], [534, 652], [528, 644], [529, 645], | [368, 643], <~>, **, {21} |, | [377, 1184], <~>, **, {19, 23} |"
  val REFIT_WHOLE_T4 = "| [926, 1153], <->, **, {8} |, | [883, 946], <~>, **, {7, 9} |, | [794, 890], <->, **, {8} |, | [606, 782], <->, **, {7, 9} |, | [434, 602], <->, **, {8} |, | [236, 444], <~>, **, {7, 9} |, | [225, 256], <~>, **, {8} |, | [225, 250], <~>, **, {7, 9} |, | [142, 248], <~>, **, {8} |, | [155, 245], <+>, **, {7, 9} |, | [237, 474], <~>, **, {8} |, | [454, 574], <~>, **, {7, 9} |, | [573, 739], <+>, **, {8} |, | [727, 829], <+>, **, {7, 9} |, | [823, 896], <~>, **, {8} |, | [876, 956], <~>, **, {7, 9} |"
  val REFIT_WHOLE_T4L = "| [333, 729], <~>, **, {32} |, | [263, 864], <~>, **, {30, 34} |, | [263, 625], <~>, **, {32} |, | [466, 486], <=>, **, {30, 34} |"
  val REFIT_FREE_FIXED_LEN         = "| [300, 1000], <+>, **, {5} | Threshold: 0"
  val REFIT_FREE_DYN_LEN           = "| [300, 1000], <+>, **, {4, 6} | Threshold: 0"
  val REFIT_FREE_DYN_LEN_WIDE      = "| [300, 1000], <+>, **, {4, 9} | Threshold: 0"
  val REFIT_BOUNDED_FIXED_LEN      = "| [300, 1000], <+>, **, {5} | @[0, 31] Threshold: 0"
  val REFIT_BOUNDED_DYN_LEN        = "| [300, 1000], <+>, **, {4, 6} | @[0, 31] Threshold: 0"
  val REFIT_FIXED_START_FIXED_LEN  = "| [300, 1000], <+>, **, {5} | @21 Threshold: 0"
  val REFIT_FIXED_START_DYN_LEN    = "| [300, 1000], <+>, **, {4, 6} | @21 Threshold: 0"
  val REFIT_MULTI_ADJACENT         = "| [300, 1000], <+>, **, {5} |, #0#, | [300, 1000], <->, **, {7} | Threshold: 0"
  val REFIT_MULTI_FIXED_GAP        = "| [300, 1000], <+>, **, {5} |, #5#, | [300, 1000], <->, **, {7} | Threshold: 0"
  val REFIT_MULTI_RANGE_GAP        = "| [300, 1000], <+>, **, {5} |, #3-8#, | [300, 1000], <->, **, {7} | Threshold: 0"
  val REFIT_MULTI_ORDERED_FLEX     = "| [300, 1000], <+>, **, {5} |, ...., | [300, 1000], <->, **, {7} | Threshold: 0"
  val REFIT_MULTI_UNORDERED_FLEX   = "UNORDERED: | [300, 1000], <+>, **, {5} |, ...., | [300, 1000], <->, **, {7} | Threshold: 0"
  val REFIT_STEP_INCREASE          = "| [300, 1000], <+>, 10, {5} | Threshold: 2"
  val REFIT_STEP_FLAT              = "| [500, 1000], <=>, **, {16} | Threshold: 2"
  val REFIT_WHOLE_CASES: Map[String, String] = Map(
    "T1" -> REFIT_WHOLE_T1, "T2" -> REFIT_WHOLE_T2, "T3" -> REFIT_WHOLE_T3,
    "T3L" -> REFIT_WHOLE_T3L, "T4" -> REFIT_WHOLE_T4, "T4L" -> REFIT_WHOLE_T4L)
  val REFIT_SUBSEQUENCE_CASES: Map[String, String] = Map(
    "FREE_FIXED_LEN" -> REFIT_FREE_FIXED_LEN, "FREE_DYN_LEN" -> REFIT_FREE_DYN_LEN,
    "FREE_DYN_LEN_WIDE" -> REFIT_FREE_DYN_LEN_WIDE, "BOUNDED_FIXED_LEN" -> REFIT_BOUNDED_FIXED_LEN,
    "BOUNDED_DYN_LEN" -> REFIT_BOUNDED_DYN_LEN, "FIXED_START_FIXED_LEN" -> REFIT_FIXED_START_FIXED_LEN,
    "FIXED_START_DYN_LEN" -> REFIT_FIXED_START_DYN_LEN, "MULTI_ADJACENT" -> REFIT_MULTI_ADJACENT,
    "MULTI_FIXED_GAP" -> REFIT_MULTI_FIXED_GAP, "MULTI_RANGE_GAP" -> REFIT_MULTI_RANGE_GAP,
    "MULTI_ORDERED_FLEX_GAP" -> REFIT_MULTI_ORDERED_FLEX, "MULTI_UNORDERED_FLEX_GAP" -> REFIT_MULTI_UNORDERED_FLEX,
    "STEP_INCREASE" -> REFIT_STEP_INCREASE, "STEP_FLAT" -> REFIT_STEP_FLAT)
  private def refitWholePatternOrEmpty: String = {
    val overridePattern = envOr("TSSEEK_QUERY_PATTERN", "")
    if (overridePattern.nonEmpty) overridePattern
    else REFIT_WHOLE_CASES.getOrElse(WHOLE_CASE.stripPrefix("REFIT_WHOLE_").toUpperCase, "")
  }
  case class DatasetParams(
    threshold: Double,
    span90: Double,
    gridW: Int,
    gridH: Double,
    yMin: Double,
    yMax: Double,
    queryPattern: String,
    epsilon: Double
  )
  def getParams(dataset: String = DATASET, alpha: Double = ALPHA): DatasetParams = {
    dataset match {

      case "TSBS" =>
        val span90 = envOr("TSSEEK_TSBS_SPAN90", "45.3").toDouble
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = envOr("TSSEEK_TSBS_GRID_W", "8").toInt,
          gridH = envOr("TSSEEK_TSBS_GRID_H", "15.0").toDouble,
          yMin = envOr("TSSEEK_TSBS_Y_MIN", "0.0").toDouble,
          yMax = envOr("TSSEEK_TSBS_Y_MAX", "50.0").toDouble,
          queryPattern = TSBS_PATTERN,
          epsilon = threshold
        )

      case "ECG" =>
        val span90 = envOr("TSSEEK_ECG_SPAN90", "0.30").toDouble
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = envOr("TSSEEK_ECG_GRID_W", "8").toInt,
          gridH = envOr("TSSEEK_ECG_GRID_H", "0.25").toDouble,
          yMin = envOr("TSSEEK_ECG_Y_MIN", "-2.0").toDouble,
          yMax = envOr("TSSEEK_ECG_Y_MAX", "3.5").toDouble,
          queryPattern = ECG_PATTERN,
          epsilon = threshold
        )
      case "RANDOMWALK" =>
        val span90 = envOr("TSSEEK_RANDOMWALK_SPAN90", "3.18").toDouble
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = envOr("TSSEEK_RANDOMWALK_GRID_W", "8").toInt,
          gridH = envOr("TSSEEK_RANDOMWALK_GRID_H", "0.25").toDouble,
          yMin = envOr("TSSEEK_RANDOMWALK_Y_MIN", "-10.0").toDouble,
          yMax = envOr("TSSEEK_RANDOMWALK_Y_MAX", "10.0").toDouble,
          queryPattern = RANDOMWALK_PATTERN,
          epsilon = threshold
        )

      case "REFIT" =>
        val span90 = requiredEnv("TSSEEK_REFIT_SPAN90").toDouble
        val threshold = alpha * span90
        DatasetParams(
          threshold = threshold,
          span90 = span90,
          gridW = requiredEnv("TSSEEK_REFIT_GRID_W").toInt,
          gridH = requiredEnv("TSSEEK_REFIT_GRID_H").toDouble,
          yMin = requiredEnv("TSSEEK_REFIT_Y_MIN").toDouble,
          yMax = requiredEnv("TSSEEK_REFIT_Y_MAX").toDouble,
          queryPattern = refitWholePatternOrEmpty,
          epsilon = threshold
        )
      case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset")
    }
  }

  def getPattern(dataset: String = DATASET, matchingType: String = MATCHING_TYPE): String = {
    val overridePattern = envOr("TSSEEK_QUERY_PATTERN", "")
    if (overridePattern.nonEmpty) {
      println(s"!!! TSSEEK_QUERY_PATTERN override in effect ($dataset): $overridePattern")
      return overridePattern
    }
    matchingType match {
      case "WHOLE_SEQUENCE" =>
        dataset match {
          case "TSBS" => TSBS_PATTERN
          case "ECG" => ECG_PATTERN
          case "RANDOMWALK" => RANDOMWALK_PATTERN
          case "REFIT" =>
            REFIT_WHOLE_CASES.getOrElse(WHOLE_CASE.stripPrefix("REFIT_WHOLE_").toUpperCase,
              throw new IllegalArgumentException(
                s"REFIT whole matching needs TSSEEK_WHOLE_CASE in ${REFIT_WHOLE_CASES.keys.toSeq.sorted.mkString(", ")} (got '$WHOLE_CASE')"))
          case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset")
        }
      case "SUBSEQUENCE" =>
        getSubsequencePattern(dataset, SUBSEQUENCE_CASE)
      case _ => throw new IllegalArgumentException(s"Unknown matching type: $matchingType")
    }
  }

  def getSubsequencePattern(dataset: String = DATASET, subCase: String = SUBSEQUENCE_CASE): String = {
    val overridePattern = envOr("TSSEEK_QUERY_PATTERN", "")
    if (overridePattern.nonEmpty) {
      println(s"!!! TSSEEK_QUERY_PATTERN override in effect ($dataset): $overridePattern")
      return overridePattern
    }
    (dataset, subCase) match {
      case ("ECG", "FREE_FIXED_LEN")          => ECG_FREE_FIXED_LEN
      case ("ECG", "FREE_DYN_LEN")            => ECG_FREE_DYN_LEN
      case ("ECG", "FREE_DYN_LEN_WIDE")       => ECG_FREE_DYN_LEN_WIDE
      case ("ECG", "BOUNDED_FIXED_LEN")       => ECG_BOUNDED_FIXED_LEN
      case ("ECG", "BOUNDED_DYN_LEN")         => ECG_BOUNDED_DYN_LEN
      case ("ECG", "FIXED_START_FIXED_LEN")   => ECG_FIXED_START_FIXED_LEN
      case ("ECG", "FIXED_START_DYN_LEN")     => ECG_FIXED_START_DYN_LEN
      case ("ECG", "MULTI_ADJACENT")          => ECG_MULTI_ADJACENT
      case ("ECG", "MULTI_FIXED_GAP")         => ECG_MULTI_FIXED_GAP
      case ("ECG", "MULTI_RANGE_GAP")         => ECG_MULTI_RANGE_GAP
      case ("ECG", "MULTI_ORDERED_FLEX_GAP")  => ECG_MULTI_ORDERED_FLEX
      case ("ECG", "MULTI_UNORDERED_FLEX_GAP") => ECG_MULTI_UNORDERED_FLEX
      case ("ECG", "1A") => ECG_SUBSEQUENCE_1A_PATTERN
      case ("ECG", "1B") => ECG_SUBSEQUENCE_1B_PATTERN
      case ("ECG", "1C") => ECG_SUBSEQUENCE_PATTERN
      case ("ECG", "2A") => ECG_SUBSEQUENCE_2A_PATTERN
      case ("ECG", "2B") => ECG_SUBSEQUENCE_2B_PATTERN
      case ("ECG", "2C") => ECG_SUBSEQUENCE_2C_PATTERN

      case ("TSBS", "1A") | ("TSBS", "FIXED_START_FIXED_LEN") => TSBS_SUBSEQUENCE_1A_PATTERN
      case ("TSBS", "1B") | ("TSBS", "BOUNDED_FIXED_LEN")     => TSBS_SUBSEQUENCE_1B_PATTERN
      case ("TSBS", "1C") | ("TSBS", "FREE_FIXED_LEN")        => TSBS_SUBSEQUENCE_PATTERN
      case ("TSBS", "2A")                                     => TSBS_SUBSEQUENCE_2A_PATTERN
      case ("TSBS", "2B") | ("TSBS", "MULTI_FIXED_GAP")       => TSBS_SUBSEQUENCE_2B_PATTERN
      case ("TSBS", "2C") | ("TSBS", "MULTI_ORDERED_FLEX_GAP") => TSBS_SUBSEQUENCE_2C_PATTERN
      case ("RANDOMWALK", "1A") | ("RANDOMWALK", "FIXED_START_FIXED_LEN") => RANDOMWALK_SUBSEQUENCE_1A_PATTERN
      case ("RANDOMWALK", "1B") | ("RANDOMWALK", "BOUNDED_FIXED_LEN")     => RANDOMWALK_SUBSEQUENCE_1B_PATTERN
      case ("RANDOMWALK", "1C") | ("RANDOMWALK", "FREE_FIXED_LEN")        => RANDOMWALK_SUBSEQUENCE_PATTERN
      case ("RANDOMWALK", "2A")                                           => RANDOMWALK_SUBSEQUENCE_2A_PATTERN
      case ("RANDOMWALK", "2B") | ("RANDOMWALK", "MULTI_FIXED_GAP")       => RANDOMWALK_SUBSEQUENCE_2B_PATTERN
      case ("RANDOMWALK", "2C") | ("RANDOMWALK", "MULTI_ORDERED_FLEX_GAP") => RANDOMWALK_SUBSEQUENCE_2C_PATTERN
      case ("REFIT", c) if REFIT_SUBSEQUENCE_CASES.contains(c) => REFIT_SUBSEQUENCE_CASES(c)
      case _ => throw new IllegalArgumentException(s"Unknown dataset: $dataset or subsequence case: $subCase")
    }
  }

  def getGroundTruthSeeds(dataset: String = DATASET, datasetSize: String = DATASET_SIZE): Set[Long] = {
    val overrideIds = envOr("TSSEEK_GROUND_TRUTH_SEEDS", "")
    if (overrideIds.nonEmpty && dataset == "REFIT") {
      println(s"!!! TSSEEK_GROUND_TRUTH_SEEDS override in effect (REFIT): ${overrideIds.split(",").length} ids")
      return overrideIds.split(",").map(_.trim.toLong).toSet
    } else if (overrideIds.nonEmpty) {
      println(s"!!! TSSEEK_GROUND_TRUTH_SEEDS is set but ignored for dataset $dataset (REFIT-only override)")
    }
    dataset match {
      case "TSBS" => Set(
        750000L, 750001L, 750002L, 750003L, 750004L, 750005L, 750006L, 750007L, 750008L, 750009L,
        750010L, 750011L, 750012L, 750013L, 750014L, 750015L, 750016L, 750017L, 750018L, 750019L,
        750020L, 750021L, 750022L, 750023L, 750024L
      )
      case "ECG" =>
        datasetSize match {
          case "25m" =>
            Set(
              625000L, 625001L, 625002L, 625003L, 625004L, 625005L, 625006L, 625007L, 625008L, 625009L,
              625010L, 625011L, 625012L, 625013L, 625014L, 625015L, 625016L, 625017L, 625018L, 625019L,
              625020L, 625021L, 625022L, 625023L, 625024L
            )
          case "50m" =>
            Set(
              625000L, 625001L, 625002L, 625003L, 625004L, 625005L, 625006L, 625007L, 625008L, 625009L,
              625010L, 625011L, 625012L, 625013L, 625014L, 625015L, 625016L, 625017L, 625018L, 625019L,
              625020L, 625021L, 625022L, 625023L, 625024L, 625025L, 625026L, 625027L, 625028L, 625029L,
              625030L, 625031L, 625032L, 625033L, 625034L, 625035L, 625036L, 625037L, 625038L, 625039L,
              625040L, 625041L, 625042L, 625043L, 625044L, 625045L, 625046L, 625047L, 625048L, 625049L
            )
          case _ =>
            Set(
              625000L, 625001L, 625002L, 625003L, 625004L, 625005L, 625006L, 625007L, 625008L, 625009L,
              625010L, 625011L, 625012L, 625013L, 625014L, 625015L, 625016L, 625017L, 625018L, 625019L,
              625020L, 625021L, 625022L, 625023L, 625024L
            )
        }
      case "RANDOMWALK" => Set(
        0L, 102L, 204L, 306L, 408L, 510L, 612L, 714L, 816L, 918L,
        1020L, 1122L, 1224L, 1326L, 1428L, 1530L, 1632L, 1734L, 1836L, 1938L,
        2040L, 2142L, 2244L, 2346L, 2448L, 2550L, 2652L, 2754L, 2856L, 2958L,
        3060L, 3162L, 3264L, 3366L, 3468L, 3570L, 3672L, 3774L, 3876L, 3978L,
        4080L, 4182L, 4284L, 4386L, 4488L, 4590L, 4692L, 4794L, 4896L, 4998L,
        5100L, 5202L, 5304L, 5406L, 5508L, 5610L, 5712L, 5814L, 5916L, 6018L,
        6120L, 6222L, 6324L, 6426L, 6528L, 6630L, 6732L, 6834L, 6936L, 7038L,
        7140L, 7242L, 7344L, 7446L, 7548L, 7650L, 7752L, 7854L, 7956L, 8058L,
        8160L, 8262L, 8364L, 8466L, 8568L, 8670L, 8772L, 8874L, 8976L, 9078L,
        9180L, 9282L, 9384L, 9486L, 9588L, 9690L, 9792L, 9894L, 9996L, 10098L
      )
      case _ => Set.empty[Long]
    }
  }
}
