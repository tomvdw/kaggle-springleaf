package tom.kaggle.springleaf.preprocess

import org.scalatest._
import tom.kaggle.springleaf.Names
import tom.kaggle.springleaf.analysis.ColumnValueAnalyzer

class SqlDataTypeTransformerSpec extends FlatSpec with Matchers {
  val columnName = "column_x"

  "Extract a boolean" should "have a bool prefix" in {
    val extractBoolean: List[String] = SqlDataTypeTransformer.extractBoolean(columnName)
    extractBoolean.length should be (1)
    extractBoolean.head.contains(s"AS ${Names.PrefixOfString}_${columnName}") should be (true)
  }

  "Extract a categorical value" should "group infrequent values" in {
    val frequent: String = "frequent"
    val infrequent1: String = "infrequent1"
    val infrequent2: String = "infrequent2"

    val valueCounts: Map[String, Long] = Map(frequent -> 100, infrequent1 -> 2, infrequent2 -> 3)
    val analysis = ColumnValueAnalyzer(valueCounts, 105)
    val extractedValue = SqlDataTypeTransformer.extractCategoricalValue(columnName, analysis)

    extractedValue.size should be (1)
    extractedValue.head.contains(s"IN ('$infrequent1','$infrequent2')")
  }
}
