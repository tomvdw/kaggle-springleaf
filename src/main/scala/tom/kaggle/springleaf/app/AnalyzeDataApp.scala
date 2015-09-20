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

    val inferredTypes: Map[String, DataType] = {
      readInferredTypes().getOrElse {
        val categoricalVariables = schemaInspector.getCategoricalVariables
        println("%d number of categoricalVariables".format(categoricalVariables.length))
        val columnValues = ac.cachedAnalysis.analyze(categoricalVariables)
        val inferredTypes = ac.typeInference.inferTypes(columnValues)
        inferredTypes.foreach {
          case (column, dataType) => println(s"Inferred type ${dataType.typeName} for column $column")
        }
        saveInferredTypes(inferredTypes)
        inferredTypes
      }
    }

    val selectExpressionsCategorical = inferredTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
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
    val features = FeatureVectorCreator(df).getFeatureVectors
    try {
      features.saveAsObjectFile(ac.trainFeatureVectorPath)
    } catch {
      case e: Throwable => println("Could not store feature vectors. Probably they already exist.")
    }
    features
  }

  private def saveInferredTypes(inferredTypes: Map[String, DataType]) {
    val outputFile = new File(ac.cachedInferrededTypesPath)
    if (outputFile.exists()) outputFile.delete()
    val writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))
    inferredTypes.foreach { case (column, inferredType) => writer.println(s"$column = ${inferredType.json}") }
    writer.close()
  }

  private def readInferredTypes(): Option[Map[String, DataType]] = {
    val file = new File(ac.cachedInferrededTypesPath)
    if (file.exists()) {
      val lines = scala.io.Source.fromFile(file).getLines()
      val map: Map[String, DataType] = lines.map { line =>
        val parts = line.split(" = ", 2)
        val inferredType = DataType.fromJson(parts(1))
        parts(0) -> inferredType
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
