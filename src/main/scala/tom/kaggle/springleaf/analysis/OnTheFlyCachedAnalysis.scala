package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

case class OnTheFlyCachedAnalysis(sqlContext: SQLContext, table: String) extends ICachedAnalysis {
  override def readColumnValueCounts: Map[String, Map[String, Long]] = Map()

  override def analyze(variables: Array[StructField]): Map[String, Map[String, Long]] = {
    val df = sqlContext.table(table)
    val columns = variables.flatMap(variable => List(
      approxCountDistinct(variable.name).as(s"DISTINCT_${variable.name}"),
      min(variable.name).as(s"MIN_${variable.name}")))
    val results = df.agg(columns.head, columns.tail: _*)


    ???
  }
}
