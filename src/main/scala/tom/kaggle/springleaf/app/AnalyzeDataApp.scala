package tom.kaggle.springleaf.app

import tom.kaggle.springleaf.ApplicationContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import tom.kaggle.springleaf.KeyHelper

case class AnalyzeDataApp(ac: ApplicationContext) {

  def run {
    analyzeCategoricalVariables
  }

  private def analyzeCategoricalVariables {
    val variables = ac.schemaInspector.getCategoricalVariables
    val columnValues = ac.cachedAnalysis.analyze(variables)
//        val columnValues = ac.cachedAnalysis.readColumnValueCounts
    val predictedTypes = ac.analyzer.predictType(columnValues)
    predictedTypes.foreach {
      case (column, t) => println(column + ": " + t)
    }

    predictedTypes.map(pt => castColumn(pt._1, pt._2)).foreach(println)
  }
  
  private def castColumn(column: String, dataType: DataType): String = {
    val castedColumn = castedColumnName(column)
    dataType match {
      case IntegerType => "cast(%s AS DECIMAL) as %s".format(column, castedColumn)
      case DoubleType => "cast(%s AS DECIMAL) as %s".format(column, castedColumn)
      case default => column
    }
  }

  private def castedColumnName(column: String) = "C_" + column
}

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext
    val app = AnalyzeDataApp(ac)
    app.run
  }

}