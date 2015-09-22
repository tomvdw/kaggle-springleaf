package tom.kaggle.springleaf

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, LongType}

object SqlDataTypeTransformer {
  def castColumn(column: String, dataType: DataType): List[String] = {
    dataType match {
      case IntegerType => extractDecimal(column)
      case LongType => extractDecimal(column)
      case DoubleType => extractDecimal(column)
      case DateType => extractStandardDateFields(column)
      case BooleanType => extractBoolean(column)
      case default => List(s"$column AS ${ApplicationContext.prefixOfString}_$column")
    }
  }

  def extractDateField(column: String, index: Int): String = s"REGEXP_EXTRACT($column, '(\\d{2})([A-Z]{3})(\\d{2}):(\\d{2}):(\\d{2}):(\\d{2})', $index)"

  def extractStandardDateFields(column: String): List[String] = {
    List(decimal(extractDateField(column, 3), s"${ApplicationContext.prefixOfDecimal}_${column}_DATE_YEAR"),
      string(extractDateField(column, 2), s"${ApplicationContext.prefixOfString}_${column}_DATE_MONTH"),
      decimal(extractDateField(column, 1), s"${ApplicationContext.prefixOfDecimal}_${column}_DATE_DAY"),
      decimal(extractDateField(column, 4), s"${ApplicationContext.prefixOfDecimal}_${column}_DATE_HOUR"))
  }

  def decimal(expression: String, resultName: String): String = s"cast($expression AS DECIMAL) as $resultName"

  def string(expression: String, resultName: String): String = s"$expression as $resultName"

  def extractDecimal(column: String): List[String] = {
    List(s"CASE WHEN $column IS NULL OR $column = '' THEN NULL ELSE cast($column AS DECIMAL) END as ${ApplicationContext.prefixOfDecimal}_${column}")
  }

  def extractBoolean(column: String): List[String] = {
    List(s"CASE WHEN $column = 'false' THEN 0 WHEN $column = 'true' THEN 1 ELSE NULL END AS ${ApplicationContext.prefixOfBool}_${column}")
  }
}