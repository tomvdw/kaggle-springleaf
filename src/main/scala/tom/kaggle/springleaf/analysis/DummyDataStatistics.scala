package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.StructField

class DummyDataStatistics extends IDataStatistics{
  override def valueCount(column: StructField): Array[(String, Long)] = ???
}
