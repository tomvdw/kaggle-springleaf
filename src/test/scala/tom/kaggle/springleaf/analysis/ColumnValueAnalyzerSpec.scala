package tom.kaggle.springleaf.analysis

import org.scalatest.{FlatSpec, Matchers}

class ColumnValueAnalyzerSpec extends FlatSpec with Matchers {
  val v1 = 1.0
  val v2 = 2.0
  val v3 = 3.0
  val v4 = 4.0

  "percentile of a single value" should "always return that value" in {
    val c1 = 10
    val valueCounts: Map[String, Long] = Map(v1.toString -> c1)
    val totalNumberOfRecords = c1
    val analyzer = ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)

    for (tile <- 0 to 100) analyzer.percentile(tile) should be (v1)
  }

  "percentile of two equally often occurring values" should "get the right result" in {
    val c1 = 10
    val c2 = 10
    val valueCounts: Map[String, Long] = Map(v1.toString -> c1, v2.toString -> c2)
    val totalNumberOfRecords = c1 + c2
    val analyzer = ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)

    for (tile <- 0 to 50) analyzer.percentile(tile) should be (v1)
    for (tile <- 51 to 100) analyzer.percentile(tile) should be (v2)
  }

  "percentile of four values" should "get the right result" in {
    val c1 = 1
    val c2 = 49
    val c3 = 49
    val c4 = 1
    val valueCounts: Map[String, Long] = Map(v1.toString -> c1, v2.toString -> c2, v3.toString -> c3, v4.toString -> c4)
    val totalNumberOfRecords = c1 + c2 + c3 + c4
    val analyzer = ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)

    analyzer.percentile(0) should be (v1)
    analyzer.percentile(1) should be (v1)
    for (tile <- 2 to 50) analyzer.percentile(tile) should be (v2)
    for (tile <- 51 to 99) analyzer.percentile(tile) should be (v3)
    analyzer.percentile(100) should be (v4)
  }

}
