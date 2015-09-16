package tom.kaggle.springleaf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType

object SqlDataTypeTransformer {
  def castColumn(column: String, dataType: DataType): List[String] = {
    dataType match {
      case IntegerType => extractDecimal(column)
      case LongType    => extractDecimal(column)
      case DoubleType  => extractDecimal(column)
      case DateType    => extractStandardDateFields(column)
      case BooleanType => extractBoolean(column)
      case default     => List(column + " AS STR_" + column)
    }
  }

  def extractDateField(column: String, index: Int): String = {
    "REGEXP_EXTRACT(%s, '(\\d{2})([A-Z]{3})(\\d{2}):(\\d{2}):(\\d{2}):(\\d{2})', %d)"
      .format(column, index)
  }

  def extractStandardDateFields(column: String): List[String] = {
    List(decimal(extractDateField(column, 3), "DEC_" + column + "_DATE_YEAR"),
      string(extractDateField(column, 2), "STR_" + column + "_DATE_MONTH"),
      decimal(extractDateField(column, 1), "DEC_" + column + "_DATE_DAY"),
      decimal(extractDateField(column, 4), "DEC_" + column + "_DATE_HOUR"))
  }

  def decimal(expression: String, resultName: String): String = "cast(%s AS DECIMAL) as %s".format(expression, resultName)

  def string(expression: String, resultName: String): String =
    "%s as %s".format(expression, resultName)

  def extractDecimal(column: String): List[String] = {
    List("CASE WHEN %s IS NULL OR %s = '' THEN NULL ELSE cast(%s AS DECIMAL) END as %s"
      .format(column, column, column, "DEC_" + column))
  }

  def extractBoolean(column: String): List[String] = {
    List("CASE WHEN %s = 'false' THEN 0 WHEN %s = 'true' THEN 1 ELSE NULL END AS %s"
      .format(column, column, "BOOL_" + column))
  }
}