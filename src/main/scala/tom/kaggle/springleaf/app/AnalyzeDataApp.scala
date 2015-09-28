package tom.kaggle.springleaf.app

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf._
import tom.kaggle.springleaf.ml.FeatureVectorCreator
import tom.kaggle.springleaf.preprocess.{SqlDataTypeTransformer, ITypeInferrer}

object AnalyzeDataApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sqlContext = inject[SQLContext]
  private val df = inject[DataFrame]
  private val typeInferrer = inject[ITypeInferrer]
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")

  analyzeCategoricalVariables()

  private def analyzeCategoricalVariables() {
    val inferredTypes: Map[String, DataType] = typeInferrer.inferTypes

    val schemaInspector = SchemaInspector(df)
    val selectCategorical = schemaInspector.getCategoricalVariables.filter(c => inferredTypes.contains(c.name)).map(c => s"${c.name} as ${Names.PrefixOfString}_${c.name}")
    selectCategorical.foreach(println)
    val selectNumericalDisguisedAsCategorical = inferredTypes.flatMap(pt => SqlDataTypeTransformer.castColumn(pt._1, pt._2))
    val selectTrueNumerical = schemaInspector.getNumericalColumns.map(x => s"$x AS ${Names.PrefixOfDecimal}_$x")
    val selectAllNumerical = selectTrueNumerical ++ selectNumericalDisguisedAsCategorical
    println(s"${selectCategorical.length} categorical variables")
    println(s"${selectNumericalDisguisedAsCategorical.size} variables read as categorical, converted to numeric")
    println(s"${selectTrueNumerical.size} variables read as pure numerical")
    println(s"In total ${selectAllNumerical.size} of variables")
    val trainFeatureVectors = getFeatureVector(Names.TableName, selectAllNumerical ++ selectCategorical)
    trainFeatureVectors.take(5).foreach(println)
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

}
