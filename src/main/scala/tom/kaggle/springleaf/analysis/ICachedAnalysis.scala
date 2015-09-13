package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.StructField

trait ICachedAnalysis {
  def readColumnValueCounts: Map[String, Map[String, Long]]
  def analyze(variables: Array[StructField]): Map[String, Map[String, Long]]
}