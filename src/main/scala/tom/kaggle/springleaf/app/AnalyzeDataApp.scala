package tom.kaggle.springleaf.app

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import tom.kaggle.springleaf.ApplicationContext
import tom.kaggle.springleaf.SqlDataTypeTransformer
import org.apache.spark.sql.types.BooleanType
import tom.kaggle.springleaf.SchemaInspector
import tom.kaggle.springleaf.ml.FeatureVectorCreater

case class AnalyzeDataApp(ac: ApplicationContext) {

  def run {
    analyzeCategoricalVariables
  }

  private def analyzeCategoricalVariables {
    val df = ac.df
    val variables = SchemaInspector(df).getCategoricalVariables
    val columnValues = ac.cachedAnalysis.analyze(variables)
    val predictedTypes = ac.analyzer.predictType(columnValues)

    val selectExpressions = predictedTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val query = "SELECT %s, %s FROM %s".format(
      selectExpressions.mkString(",\n"), ApplicationContext.labelFieldName, ApplicationContext.tableName)
    val trainingSetDf = ac.sqlContext.sql(query)
    trainingSetDf.show(1) // hm, otherwise the schema seems to be null => NullPointerException
    val features = FeatureVectorCreater(trainingSetDf).getFeatureVector
    features.saveAsObjectFile(ApplicationContext.dataFolderPath + "/feature-vector" + ApplicationContext.fraction)
    
    features.take(10).foreach(println)
  }

}

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext
    val app = AnalyzeDataApp(ac)
    app.run
  }

}