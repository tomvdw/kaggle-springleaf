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
    val schemaInspector = SchemaInspector(df)
    val categoricalVariables = schemaInspector.getCategoricalVariables
    println("%d number of categoricalVariables".format(categoricalVariables.size))
    val columnValues = ac.cachedAnalysis.analyze(categoricalVariables)
    val predictedTypes = ac.analyzer.predictType(columnValues)
    println("%d number of predicted types".format(predictedTypes.size))

    val selectExpressionsCategorical = predictedTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val selectExpressionsNumerical = schemaInspector.getNumericalColumns.map(x => "%s AS %s".format(x, "DEC_" + x))
    val selectExpressionsForAllNumerical = selectExpressionsNumerical ++ selectExpressionsCategorical
    println("%d variables read as categorical, converted to numeric".format(selectExpressionsCategorical.size))
    println("%d variables read as pure numerical".format(selectExpressionsNumerical.size))
    println("In total %d of variables".format(selectExpressionsForAllNumerical.size))
    val trainFeatureVectors = getFeatureVector(ApplicationContext.tableName, selectExpressionsForAllNumerical)

    trainFeatureVectors.take(10).foreach(println)
  }

  def getFeatureVector(tableName: String, selectExpressions: Iterable[String]) = {
    val query = "SELECT %s, %s FROM %s".format(
      selectExpressions.mkString(",\n"), ApplicationContext.labelFieldName, tableName)
    val df = ac.sqlContext.sql(query)
    df.show(1) // hm, otherwise the schema seems to be null => NullPointerException
    val features = FeatureVectorCreater(df).getFeatureVector
    features.saveAsObjectFile(ApplicationContext.trainFeatureVectorPath)
    features
  }
}

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext
    val app = AnalyzeDataApp(ac)
    app.run
  }

}