package tom.kaggle.springleaf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType

object SqlDataTypeTransformer {
  def castColumn(column: String, dataType: DataType): List[String] = {
    dataType match {
      case IntegerType => extractDecimal(column)
      case DoubleType  => extractDecimal(column)
      case DateType    => extractStandardDateFields(column)
      case BooleanType => extractBoolean(column)
      case default     => List(column + " AS STR_" + column)
    }
  }

  def extractDateField(column: String, index: Int, resultName: String): String = {
    "REGEXP_EXTRACT(%s, '(\\d{2})([A-Z]{3})(\\d{2}):(\\d{2}):(\\d{2}):(\\d{2})', %d) as %s"
      .format(column, index, resultName)
  }

  def extractStandardDateFields(column: String): List[String] = {
    List(extractDateField(column, 3, "DATE_" + column + "_YEAR"),
      extractDateField(column, 2, "DATE_" + column + "_MONTH"),
      extractDateField(column, 1, "DATE_" + column + "_DAY"),
      extractDateField(column, 4, "DATE_" + column + "_HOUR"))
  }

  def extractDecimal(column: String): List[String] = {
    List("CASE WHEN %s IS NULL OR %s = '' THEN NULL ELSE cast(%s AS DECIMAL) END as %s"
      .format(column, column, column, "DEC_" + column))
  }

  def extractBoolean(column: String): List[String] = {
    List("CASE WHEN %s = 'false' THEN 0 WHEN %s = 'true' THEN 1 ELSE NULL END AS %s"
      .format(column, column, "BOOL_" + column))
  }
}