package tom.kaggle.springleaf.app

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf._
import tom.kaggle.springleaf.analysis.{ColumnTypeInference, ICachedAnalysis}
import tom.kaggle.springleaf.ml.FeatureVectorCreator

object AnalyzeDataApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sqlContext = inject[SQLContext]
  private val cacheAnalysis = inject[ICachedAnalysis]
  private val typeInference = inject[ColumnTypeInference]
  private val df = inject[DataFrame]
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")
  private val cachedInferredTypesPath = inject[String]("data.path.cachedInferredTypes")

  analyzeCategoricalVariables()

  private def analyzeCategoricalVariables() {
    val schemaInspector = SchemaInspector(df)

    val inferredTypes: Map[String, DataType] = {
      readInferredTypes().getOrElse {
        val categoricalVariables = schemaInspector.getCategoricalVariables
        println(s"${categoricalVariables.length} number of categoricalVariables")
        val columnValues = cacheAnalysis.analyze(categoricalVariables)
        val inferredTypes = typeInference.inferTypes(columnValues)
        inferredTypes.foreach {
          case (column, dataType) => println(s"Inferred type ${dataType.typeName} for column $column")
        }
        saveInferredTypes(inferredTypes)
        inferredTypes
      }
    }

    val selectExpressionsCategorical = inferredTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val selectExpressionsNumerical = schemaInspector.getNumericalColumns.map(x => s"$x AS ${Names.PrefixOfDecimal}_$x")
    val selectExpressionsForAllNumerical = selectExpressionsNumerical ++ selectExpressionsCategorical
    println(s"${selectExpressionsCategorical.size} variables read as categorical, converted to numeric")
    println(s"${selectExpressionsNumerical.size} variables read as pure numerical")
    println(s"In total ${selectExpressionsForAllNumerical.size} of variables")
    val trainFeatureVectors = getFeatureVector(Names.TableName, selectExpressionsForAllNumerical)

    //trainFeatureVectors.take(5).foreach(println)
  }

  private def getFeatureVector(tableName: String, selectExpressions: Iterable[String]) = {
    val query = s"SELECT ${selectExpressions.mkString(",\n")}, ${Names.LabelFieldName} FROM $tableName"
    val df = sqlContext.sql(query)
    df.show(1) // hm, otherwise the schema seems to be null => NullPointerException

    val dfWithIndexedCategoricalVariables = IndexedCategoricalVariableCreator(df).transformedDf
    dfWithIndexedCategoricalVariables.show(1)
    val features = FeatureVectorCreator(dfWithIndexedCategoricalVariables).getFeatureVectors
    try {
      features.saveAsObjectFile(trainFeatureVectorPath)
    } catch {
      case e: Throwable => println("Could not store feature vectors. Probably they already exist.")
    }
    features
  }

  private def saveInferredTypes(inferredTypes: Map[String, DataType]) {
    val outputFile = new File(cachedInferredTypesPath)
    if (outputFile.exists()) outputFile.delete()
    val writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))
    inferredTypes.foreach { case (column, inferredType) => writer.println(s"$column = ${inferredType.json}") }
    writer.close()
  }

  private def readInferredTypes(): Option[Map[String, DataType]] = {
    val file = new File(cachedInferredTypesPath)
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
