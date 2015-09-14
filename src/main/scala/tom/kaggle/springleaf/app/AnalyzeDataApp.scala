package tom.kaggle.springleaf.app

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import tom.kaggle.springleaf.ApplicationContext
import tom.kaggle.springleaf.SqlDataTypeTransformer
import org.apache.spark.sql.types.BooleanType

case class AnalyzeDataApp(ac: ApplicationContext) {

  def run {
    analyzeCategoricalVariables
  }

  private def analyzeCategoricalVariables {
    val variables = ac.schemaInspector.getCategoricalVariables
    val columnValues = ac.cachedAnalysis.analyze(variables)
    val predictedTypes = ac.analyzer.predictType(columnValues)

    val selectExpressions = predictedTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val df = ac.df
    val query = "SELECT %s FROM %s".format(selectExpressions.mkString(",\n"), ApplicationContext.tableName)
    ac.sqlContext.sql(query).show()
  }

}

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext
    val app = AnalyzeDataApp(ac)
    app.run
  }

}