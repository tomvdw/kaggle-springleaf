package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.StructField

trait IDataStatistics {
  def valueCount(column: StructField): Array[(String, Long)]
}
