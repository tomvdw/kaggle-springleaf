package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, StringType, StructField}
import tom.kaggle.springleaf.ApplicationContext

import scala.util.matching.Regex

case class CategoricalColumnAnalyzer(
    ac: ApplicationContext) {

  def predictType(valuesPerColumn: Map[String, Map[String, Long]]): Map[String, DataType] = {
    valuesPerColumn.map {
      case (column, values) =>
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

        column -> predictedType
    }
  }

  def isRemovable(values: Array[(String, Long)]): Boolean = {
    val total = values.map(_._2).sum
    val minimum = math.max(1, total / 100)
    if (values.length == 1) true
    else if (values.length == 2) {
      values(0)._2 < minimum || values(1)._2 < minimum
    } else false
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
