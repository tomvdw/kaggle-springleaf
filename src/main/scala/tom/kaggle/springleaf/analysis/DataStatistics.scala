package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField

class DataStatistics(sqlContext: SQLContext, table: String) {

  def valueCount(column: StructField): Array[(String, Long)] =
    sqlContext.sql(s"select ${column.name}, count(1) from $table group by ${column.name}")
      .map(r => r.getString(0) -> r.getLong(1))
      .collect()
}
