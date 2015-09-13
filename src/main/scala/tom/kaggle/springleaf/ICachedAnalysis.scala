package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

trait ICachedAnalysis {
  def readColumnValueCounts: Map[String, Map[String, Long]]
  def analyze(variables: Array[StructField]): Map[String, Map[String, Long]]
}