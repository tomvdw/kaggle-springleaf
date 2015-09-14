package tom.kaggle.springleaf.analysis

import scala.util.matching.Regex
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import tom.kaggle.springleaf.ApplicationContext

case class CategoricalColumnAnalyzer(
    ac: ApplicationContext) {

  def predictType(valuesPerColumn: Map[String, Map[String, Long]]): Map[String, DataType] = {
    valuesPerColumn.map {
      case (column, values) => {
        val counter = countMatches(values)_
        val nrOfIntegers = counter(ApplicationContext.integerRegex)
        val nrOfDoubles = counter(ApplicationContext.doubleRegex)

        val minimum = math.max(1, values.size / 2)
        val predictedType =
          if (nrOfIntegers >= minimum) IntegerType
          else if (nrOfDoubles + nrOfIntegers >= minimum) DoubleType
          else if (counter(ApplicationContext.dateRegex) >= minimum) DateType
          else if (counter(ApplicationContext.booleanRegex) >= minimum) BooleanType
          else {
            println("Defaulted to StringTyoe for %s! Values: %s".format(column, values))
            StringType
          }

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
    } else false
  }

  def getValueCounts(table: String, column: StructField): Array[(String, Long)] = {
    val results = ac.sqlContext.sql(
      "SELECT %s as v, count(1) as c FROM %s GROUP BY %s"
        .format(column.name, table, column.name))
    results.map { row => (row.getAs[String]("v") -> row.getAs[Long]("c")) }.collect()
  }

  private def countMatches(values: Map[String, Long])(regex: Regex): Int = {
    values.map {
      case (value, occurrences) =>
        regex.findFirstIn(value) match {
          case Some(x) => 1
          case None    => 0
        }
    }.sum
  }
}
