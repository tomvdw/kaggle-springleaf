package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, StringType}

import scala.util.matching.Regex

class ColumnTypeInference {

  def inferTypes(valuesPerColumn: Map[String, Map[String, Long]], totalNumberOfRecords: Long): Map[String, DataType] =
    valuesPerColumn.mapValues { occurrencesPerValue =>
      def count(regex: Regex) = occurrencesPerValue.filterKeys(regex.findFirstIn(_).nonEmpty).values.sum
      lazy val emptyStringCount = count(ColumnTypeInference.EmptyStringRegex)
      val target = totalNumberOfRecords - emptyStringCount

      lazy val intCount = count(ColumnTypeInference.IntegerRegex)
      lazy val doubleCount = count(ColumnTypeInference.DoubleRegex)
      lazy val dateCount = count(ColumnTypeInference.DateRegex)
      lazy val boolCount = count(ColumnTypeInference.BooleanRegex)

      if (intCount == target) IntegerType
      else if (doubleCount + intCount == target) DoubleType
      else if (dateCount == target) DateType
      else if (boolCount == target) BooleanType
      else StringType
    }
}

object ColumnTypeInference {
  val IntegerRegex = "^-?\\d+$".r
  val DoubleRegex = "^-?\\d+\\.\\d+$".r
  val DateRegex = "^\\d{2}[A-Z]{3}\\d{2}".r
  val BooleanRegex = "^(false|true)$".r
  val EmptyStringRegex = "^$".r
}
