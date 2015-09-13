package tom.kaggle.springleaf

import scala.util.matching.Regex
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

case class CategoricalColumnAnalyzer(
    ac: ApplicationContext,
    df: DataFrame) {

  val integerRegex = "^-?\\d+$".r
  val doubleRegex = "^-?\\d+\\.\\d+$".r
  val dateRegex = "^\\d{2}[A-Z]{3}\\d{2}".r
  val booleanRegex = "^(false|true)$".r

  def predictType(valuesPerColumn: Map[String, Array[String]]): Map[String, Option[DataType]] = {
    valuesPerColumn.map {
      case (column, values) => {
        val counter = countMatches(values)_

        val nrOfIntegers = counter(integerRegex)
        val nrOfDoubles = counter(doubleRegex)

        val minimum = math.max(1, values.length - 1 / 2)
        val predictedType =
          if (nrOfIntegers >= minimum) Some(IntegerType)
          else if (nrOfDoubles + nrOfIntegers >= minimum) Some(DoubleType)
          else if (counter(dateRegex) >= minimum) Some(DateType)
          else if (counter(booleanRegex) >= minimum) Some(BooleanType)
          else None
        (column -> predictedType)
      }
    }
  }
  
  def isRemovable(values: Array[(String, Long)]): Boolean = {
    val total = values.map(_._2).sum
    val minimum = math.max(1, total / 100)
    if (values.size == 1) true
    else if (values.size == 2) {
      values(0)._2 < minimum || values(1)._2 < minimum
    }
    else false
  }

  def getValueCounts(table: String, column: StructField): Array[(String, Long)] = {
    val results = ac.sqlContext.sql(
      "SELECT %s as v, count(1) as c FROM %s GROUP BY %s"
        .format(column.name, table, column.name))
    results.map { row => (row.getAs[String]("v") -> row.getAs[Long]("c")) }.collect()
  }

  private def countMatches(values: Array[String])(regex: Regex): Int = {
    values.map(value => {
      regex.findFirstIn(value) match {
        case Some(x) => 1
        case None    => 0
      }
    }).sum
  }
}