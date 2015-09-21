package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, StringType}

import scala.util.matching.Regex

class ColumnTypeInference {

  def inferTypes(valuesPerColumn: Map[String, Map[String, Long]]): Map[String, DataType] =
    valuesPerColumn.mapValues { occurrencesPerValue =>
      def count(regex: Regex) = occurrencesPerValue.filterKeys(regex.findFirstIn(_).nonEmpty).values.sum
      val threshold = math.max(1, totalNumberOfOccurrences(occurrencesPerValue) / 2)

      lazy val intCount = count(ColumnTypeInference.IntegerRegex)
      lazy val doubleCount = count(ColumnTypeInference.DoubleRegex)
      lazy val dateCount = count(ColumnTypeInference.DateRegex)
      lazy val boolCount = count(ColumnTypeInference.BooleanRegex)

      if (intCount >= threshold) IntegerType
      else if (doubleCount + intCount >= threshold) DoubleType
      else if (dateCount >= threshold) DateType
      else if (boolCount >= threshold) BooleanType
      else StringType
    }

  private def totalNumberOfOccurrences(occurrencesPerValue: Map[String, Long]) = occurrencesPerValue.values.sum
}

object ColumnTypeInference {
  val IntegerRegex = "^-?\\d+$".r
  val DoubleRegex = "^-?\\d+\\.\\d+$".r
  val DateRegex = "^\\d{2}[A-Z]{3}\\d{2}".r
  val BooleanRegex = "^(false|true)$".r
}
