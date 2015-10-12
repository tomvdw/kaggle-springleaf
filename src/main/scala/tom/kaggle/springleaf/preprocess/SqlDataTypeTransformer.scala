package tom.kaggle.springleaf.preprocess

import org.apache.spark.sql.types._
import tom.kaggle.springleaf.Names
import tom.kaggle.springleaf.analysis.ColumnValueAnalyzer

object SqlDataTypeTransformer {
  def castColumn(column: String, dataType: DataType, analysis: ColumnValueAnalyzer): List[String] = {
    if (analysis.valueCounts.size < 2) return List()

    dataType match {
      case IntegerType | LongType | DoubleType => extractDecimal(column, analysis)
      case DateType => extractStandardDateFields(column)
      case BooleanType => extractBoolean(column)
      case default => extractCategoricalValue(column, analysis)
    }
  }

  def extractCategoricalValue(column: String, analysis: ColumnValueAnalyzer, minimum: Int = 10): List[String] = {
    val infrequentValues = analysis.valueCounts.filter(_._2 < minimum).keys
    if (infrequentValues.size <= 1) List(s"$column AS ${Names.PrefixOfString}_$column")
    else {
      List(
        s"""CASE
            |WHEN $column IN (\n  \"${infrequentValues.mkString("\"\n, \"")}\"\n)
            |THEN 'infrequent-value'
            |ELSE $column
            |END AS ${Names.PrefixOfString}_$column
         """.stripMargin)
    }
  }

  def extractDateField(column: String, index: Int): String =
    s"REGEXP_EXTRACT($column, '(\\d{2})([A-Z]{3})(\\d{2}):(\\d{2}):(\\d{2}):(\\d{2})', $index)"

  def extractStandardDateFields(column: String): List[String] = {
    List(decimal(extractDateField(column, 3), s"${Names.PrefixOfDecimal}_${column}_DATE_YEAR"),
      string(extractDateField(column, 2), s"${Names.PrefixOfString}_${column}_DATE_MONTH"),
      decimal(extractDateField(column, 1), s"${Names.PrefixOfDecimal}_${column}_DATE_DAY"),
      decimal(extractDateField(column, 4), s"${Names.PrefixOfDecimal}_${column}_DATE_HOUR"))
  }

  def decimal(expression: String, resultName: String): String =
    s"cast($expression AS DECIMAL) as $resultName"

  def string(expression: String, resultName: String): String = {
    s"$expression as $resultName"
  }

  def extractDecimal(column: String, analysis: ColumnValueAnalyzer): List[String] = {
    val averageValueString = formatDecimal(analysis.average)
    val standard = List(
      decimalOrAverageIfMissing(column, averageValueString),
      isMissingValue(column) //,
      //biggerOrSmallerThanAverage(column, averageValueString)
    )

    val (biggest, secondBiggest) = analysis.twoBiggest
    if (biggest > 1000000 && biggest > 10 * secondBiggest)
      standard ++ List(replaceValueWith(column, biggest.toString, secondBiggest.toString))
    else standard
  }

  private def formatDecimal(number: Double): String = "%16.8f".format(number).replace(",", ".")

  private def biggerOrSmallerThanAverage(column: String, averageValueString: String): String = {
    s"""CASE
        |WHEN $column IS NULL OR $column = '' THEN 'missing'
        |WHEN $column <= ${averageValueString} THEN '<=m'
        |ELSE '>m'
        |END
        |AS ${Names.PrefixOfString}_${column}_VAL""".stripMargin
  }

  private def isMissingValue(column: String): String = {
    s"""CASE
        |WHEN $column IS NULL OR $column = '' THEN 1
        |ELSE 0
        |END
        |AS ${Names.PrefixOfDecimal}_${column}_MISSING""".stripMargin
  }

  private def decimalOrAverageIfMissing(column: String, averageValueString: String): String = {
    s"""CASE
        |WHEN $column IS NULL OR $column = ''
        |THEN ${averageValueString}
        |ELSE CAST($column AS DECIMAL)
        |END
        |AS ${Names.PrefixOfDecimal}_$column""".stripMargin
  }

  private def replaceValueWith(column: String, value: String, replacement: String): String = {
    s"""CASE
        |WHEN $column = '$value'
        |THEN CAST($replacement AS DECIMAL)
        |ELSE CAST($column AS DECIMAL)
        |END
        |AS ${Names.PrefixOfDecimal}_$column""".stripMargin
  }

  def extractBoolean(column: String): List[String] = {
    List(
      s"""CASE
          |WHEN $column = 'false' THEN '0'
          |WHEN $column = 'true' THEN '1'
          |ELSE ''
          |END AS ${Names.PrefixOfString}_$column""".stripMargin)
  }
}
