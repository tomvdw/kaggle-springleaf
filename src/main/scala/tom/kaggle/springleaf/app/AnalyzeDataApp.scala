package tom.kaggle.springleaf.app

import tom.kaggle.springleaf.ml.FeatureVectorCreater
import tom.kaggle.springleaf.{ApplicationContext, SchemaInspector, SqlDataTypeTransformer}

case class AnalyzeDataApp(ac: ApplicationContext) {

  def run() {
    analyzeCategoricalVariables()
  }

  private def analyzeCategoricalVariables() {
    val df = ac.df
    val schemaInspector = SchemaInspector(df)
    val categoricalVariables = schemaInspector.getCategoricalVariables
    println("%d number of categoricalVariables".format(categoricalVariables.length))
    val columnValues = ac.cachedAnalysis.analyze(categoricalVariables)
    val predictedTypes = ac.analyzer.predictType(columnValues)
    println("%d number of predicted types".format(predictedTypes.size))

    val selectExpressionsCategorical = predictedTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val selectExpressionsNumerical = schemaInspector.getNumericalColumns.map(x => "%s AS %s".format(x, "DEC_" + x))
    val selectExpressionsForAllNumerical = selectExpressionsNumerical ++ selectExpressionsCategorical
    println(s"${selectExpressionsCategorical.size} variables read as categorical, converted to numeric")
    println(s"${selectExpressionsNumerical.size} variables read as pure numerical")
    println(s"In total ${selectExpressionsForAllNumerical.size} of variables")
    val trainFeatureVectors = getFeatureVector(ApplicationContext.tableName, selectExpressionsForAllNumerical)

    trainFeatureVectors.take(10).foreach(println)
  }

  def getFeatureVector(tableName: String, selectExpressions: Iterable[String]) = {
    val query = s"SELECT ${selectExpressions.mkString(",\n")}, ${ApplicationContext.labelFieldName} FROM $tableName"
    val df = ac.sqlContext.sql(query)
    df.show(1) // hm, otherwise the schema seems to be null => NullPointerException
    val features = FeatureVectorCreater(df).getFeatureVector
    features.saveAsObjectFile(ac.trainFeatureVectorPath)
    features
  }
}

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val configFilePath = if (args.length == 0) "application.conf" else args(0)
    val ac = new ApplicationContext(configFilePath)
    val app = AnalyzeDataApp(ac)
    app.run()
  }

}