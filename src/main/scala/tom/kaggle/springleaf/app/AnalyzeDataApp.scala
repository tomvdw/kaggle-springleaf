package tom.kaggle.springleaf.app

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf._
import tom.kaggle.springleaf.analysis._
import tom.kaggle.springleaf.ml.{FeatureVector, FeatureVectorCreator}
import tom.kaggle.springleaf.preprocess.{IndexedCategoricalVariableCreator, SqlDataTypeTransformer}

import scala.collection.immutable.IndexedSeq
import scala.util.Random

object AnalyzeDataApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sqlContext = inject[SQLContext]
  private val cacheAnalysis = inject[ICachedAnalysis]
  private val typeInference = inject[ColumnTypeInference]
  private val df = inject[DataFrame]
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")
  private val cachedInferredTypesPath = inject[String]("data.path.cachedInferredTypes")
  private val numberOfLabelsPerVariablePath = inject[String]("data.path.numberOfLabelsPerVariable")
  private val labelsPerVariablePath = inject[String]("data.path.labelsPerVariable")

  analyzeCategoricalVariables()

  private def analyzeCategoricalVariables() {
    val schemaInspector = SchemaInspector(df)

    val categoricalVariables = schemaInspector.getCategoricalVariables

    println("Reading column values")
    val columnValues: Map[String, Map[String, Long]] = cacheAnalysis.analyze(categoricalVariables)

    println("Counting number of records")
    val totalNumberOfRecords = df.count()
    println(s"$totalNumberOfRecords number of records")

    println("Inferring types")
    val inferredTypes: Map[String, DataType] = inferTypes(categoricalVariables, columnValues, totalNumberOfRecords)
    println("Inferred types loaded")


    if (true) {
      println("Constructing select expressions")
      val selectExpressions: Iterable[String] = createSelectExpressions(totalNumberOfRecords, inferredTypes, columnValues)

      println("Constructing feature vectors")
      val trainFeatureVectors = getFeatureVector(Names.TableName, selectExpressions)
      trainFeatureVectors.take(5).foreach(println)
    }
  }

  private def createSelectExpressions(totalNumberOfRecords: Long, inferredTypes: Map[String, DataType], columnValues: Map[String, Map[String, Long]]): Iterable[String] = {
    val selectExpressions = inferredTypes.flatMap(pt => {
/*
      val analysis = {
        pt._2 match {
          case IntegerType | LongType | DoubleType | FloatType =>
            val valueCounts = columnValues.getOrElse(pt._1, Map())
            ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)
          case _ => ColumnValueAnalyzer(Map(), totalNumberOfRecords)
        }
      }
*/
      val valueCounts = columnValues.getOrElse(pt._1, Map())
      val analysis = ColumnValueAnalyzer(valueCounts, totalNumberOfRecords)
      SqlDataTypeTransformer.castColumn(pt._1, pt._2, analysis)
    })
    selectExpressions
  }

  private def inferTypes(categoricalVariables: Array[StructField], columnValues: Map[String, Map[String, Long]], totalNumberOfRecords: Long): Map[String, DataType] = {
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
    inferredTypes
  }

  private def normalDistributionImputation(df: DataFrame, analyses: Map[String, ColumnValueAnalyzer]): RDD[Row] = {
    val r = new Random()
    df.map(row => {
      if (row.anyNull) {
        val rowValues: IndexedSeq[Any] =
          for (i <- 0 to row.length) yield {
            val fieldName = row.schema.fieldNames(i)
            if (row.isNullAt(i) && analyses.contains(fieldName)) {
              val analysis = analyses.get(fieldName).get
              r.nextGaussian() * analysis.std + analysis.average
            }
            else row.get(i)
          }
        RowFactory.create(rowValues)
      }
      else row
    })
  }

  private def getFeatureVector(tableName: String, selectExpressions: Iterable[String]): RDD[FeatureVector] = {
    println("Executing query to get all data in the right format")
    val query = s"SELECT ${selectExpressions.mkString(",\n")}, ${Names.LabelFieldName} FROM $tableName"
    val df: DataFrame = sqlContext.sql(query)
    df.show(1) // hm, otherwise the schema seems to be null => NullPointerException
    println(s"Output of query has ${df.schema.fieldNames.length} fields")

    println("Creating indexed categorical variables")
    val indexedCategoricalVariableCreator = IndexedCategoricalVariableCreator(df)
    val dfWithIndexedCategoricalVariables = indexedCategoricalVariableCreator.transformedDf
    dfWithIndexedCategoricalVariables.show(1)
    println("Saving label meta data to files")
    IndexedCategoricalVariableCreator.saveArray(indexedCategoricalVariableCreator.models, numberOfLabelsPerVariablePath)
    IndexedCategoricalVariableCreator.saveMap(indexedCategoricalVariableCreator.models, labelsPerVariablePath)

    println("Creating feature vectors")
    val features: RDD[FeatureVector] = FeatureVectorCreator(dfWithIndexedCategoricalVariables).getFeatureVectors
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
