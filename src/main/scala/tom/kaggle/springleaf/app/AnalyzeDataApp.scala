package tom.kaggle.springleaf.app

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf._
import tom.kaggle.springleaf.analysis._
import tom.kaggle.springleaf.ml.FeatureVectorCreator
import tom.kaggle.springleaf.preprocess.{IndexedCategoricalVariableCreator, SqlDataTypeTransformer}

object AnalyzeDataApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sqlContext = inject[SQLContext]
  private val cacheAnalysis = inject[ICachedAnalysis]
  private val typeInference = inject[ColumnTypeInference]
  private val df = inject[DataFrame]
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")
  private val cachedInferredTypesPath = inject[String]("data.path.cachedInferredTypes")
  private val statistics = inject[DataStatistics]

  analyzeCategoricalVariables()

  private def analyzeCategoricalVariables() {
    val schemaInspector = SchemaInspector(df)

    val categoricalVariables = schemaInspector.getCategoricalVariables

    println("Reading column values")
    val columnValues = cacheAnalysis.analyze(categoricalVariables)

    println("Counting number of records")
    val totalNumberOfRecords = df.count()
    println(s"$totalNumberOfRecords number of records")

    println("Inferring types")
    val inferredTypes: Map[String, DataType] = {
      readInferredTypes().getOrElse {
        println(s"${categoricalVariables.length} number of categoricalVariables")
        val inferredTypes = typeInference.inferTypes(columnValues, totalNumberOfRecords)
        inferredTypes.foreach {
          case (column, dataType) => println(s"Inferred type ${dataType.typeName} for column $column")
        }
        saveInferredTypes(inferredTypes)

        inferredTypes
      }
    }



//    val variableTypeInference = new VariableTypeInference(totalNumberOfRecords)
//    variableTypeInference.getVariableTypes(inferredTypes.filter { case (v, t) => t == LongType || t == IntegerType || t == DoubleType }, columnValues) //.foreach { case (s, dt) => println(s"variable $s has data type $dt")}
    //    val var_0018: String = "VAR_0018"
    //    println(s"Determining variable type of $var_0018")
    //    variableTypeInference.getVariableType(var_0018, inferredTypes.get(var_0018).get, columnValues.get(var_0018).get)

    if (true) {
      println("Inferred types loaded")

      val selectExpressions = inferredTypes.flatMap(pt => {
        val valueCounts = columnValues.getOrElse(pt._1, Map())
        val analysis = {
          pt._2 match {
            case IntegerType | LongType | DoubleType => ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)
            case _ => ColumnValueAnalyzer(Map(), totalNumberOfRecords)
          }
        }
        SqlDataTypeTransformer.castColumn(pt._1, pt._2, analysis)
      })
      val selectExpressionsNumerical = schemaInspector.getNumericalColumns.map(x => s"$x AS ${Names.PrefixOfDecimal}_$x")
      val selectExpressionsForAllNumerical = selectExpressionsNumerical ++ selectExpressions
      println(s"${selectExpressions.size} variables read as categorical, converted to numeric")
      println(s"${selectExpressionsNumerical.size} variables read as pure numerical")
      println(s"In total ${selectExpressionsForAllNumerical.size} of variables")

      println("Constructing feature vectors")
      val trainFeatureVectors = getFeatureVector(Names.TableName, selectExpressions)
      trainFeatureVectors.take(5).foreach(println)
    }
  }

  private def getFeatureVector(tableName: String, selectExpressions: Iterable[String]) = {
    println("Executing query to get all data in the right format")
    val query = s"SELECT ${selectExpressions.mkString(",\n")}, ${Names.LabelFieldName} FROM $tableName"
    val df = sqlContext.sql(query)
    df.show(1) // hm, otherwise the schema seems to be null => NullPointerException

    println("Creating indexed categorical variables")
    val dfWithIndexedCategoricalVariables = IndexedCategoricalVariableCreator(df).transformedDf
    dfWithIndexedCategoricalVariables.show(1)

    println("Creating feature vectors")
    val features = FeatureVectorCreator(dfWithIndexedCategoricalVariables).getFeatureVectors
    try {
      val file = new File(trainFeatureVectorPath)
      if (file.exists()) FileUtils.deleteDirectory(file)
      features.saveAsObjectFile(trainFeatureVectorPath)
    } catch {
      case e: Throwable => println("Could not store feature vectors. Probably they already exist. Exception: " + e.getMessage)
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
