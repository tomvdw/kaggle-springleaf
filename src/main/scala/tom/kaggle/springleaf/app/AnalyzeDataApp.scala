package tom.kaggle.springleaf.app

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.sql.types.DataType
import tom.kaggle.springleaf.ml.FeatureVectorCreator
import tom.kaggle.springleaf.{ApplicationContext, SchemaInspector, SqlDataTypeTransformer}

case class AnalyzeDataApp(ac: ApplicationContext) {

  def run() {
    analyzeCategoricalVariables()
  }

  private def analyzeCategoricalVariables() {
    val df = ac.df
    val schemaInspector = SchemaInspector(df)

    val predictedTypes: Map[String, DataType] = {
      val cachedPredictedTypes = readPredictedTypes()
      if (cachedPredictedTypes.isDefined) cachedPredictedTypes.get
      else {
        val categoricalVariables = schemaInspector.getCategoricalVariables
        println("%d number of categoricalVariables".format(categoricalVariables.length))
        val columnValues = ac.cachedAnalysis.analyze(categoricalVariables)
        val predictedTypes = ac.analyzer.predictType(columnValues)
        savePredictedTypes(predictedTypes)
        predictedTypes
      }
    }

    val selectExpressionsCategorical = predictedTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val selectExpressionsNumerical = schemaInspector.getNumericalColumns.map(x => "%s AS %s".format(x, "DEC_" + x))
    val selectExpressionsForAllNumerical = selectExpressionsNumerical ++ selectExpressionsCategorical
    println(s"${selectExpressionsCategorical.size} variables read as categorical, converted to numeric")
    println(s"${selectExpressionsNumerical.size} variables read as pure numerical")
    println(s"In total ${selectExpressionsForAllNumerical.size} of variables")
    val trainFeatureVectors = getFeatureVector(ApplicationContext.tableName, selectExpressionsForAllNumerical)

    //trainFeatureVectors.take(5).foreach(println)
  }

  private def getFeatureVector(tableName: String, selectExpressions: Iterable[String]) = {
    val query = s"SELECT ${selectExpressions.mkString(",\n")}, ${ApplicationContext.labelFieldName} FROM $tableName"
    val df = ac.sqlContext.sql(query)
    df.show(4) // hm, otherwise the schema seems to be null => NullPointerException
    val features = FeatureVectorCreator(df).getFeatureVector
    try {
    	features.saveAsObjectFile(ac.trainFeatureVectorPath)
    } catch {
      case e: Throwable => println("Could not store feature vectors. Probably they already exist.")
    }
    features
  }

  private def savePredictedTypes(predictedTypes: Map[String, DataType]) {
    val outputFile = new File(ac.cachedPredictedTypesPath)
    if (outputFile.exists()) outputFile.delete()
    val writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))
    predictedTypes.foreach { case (column, predictedType) => writer.println(s"$column = ${predictedType.json}") }
    writer.close()
  }

  private def readPredictedTypes(): Option[Map[String, DataType]] = {
    val file = new File(ac.cachedPredictedTypesPath)
    if (file.exists()) {
      val lines = scala.io.Source.fromFile(file).getLines()
      val map: Map[String, DataType] = lines.map { line =>
        val parts = line.split(" = ", 2)
        val predictedType = DataType.fromJson(parts(1))
        parts(0) -> predictedType
      }.toMap
      Some(map)
    } else None
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