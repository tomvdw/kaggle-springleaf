package tom.kaggle.springleaf.app

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf.analysis.ICachedAnalysis
import tom.kaggle.springleaf.{SchemaInspector, SparkModule, SpringLeafModule}

object AnalyzeColumnValues extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule
  private val cacheAnalysis = inject[ICachedAnalysis]
  private val cachedInferredTypesPath = inject[String]("data.path.cachedInferredTypes")
  private val df = inject[DataFrame]

  val inferredTypes: Map[String, DataType] = readInferredTypes().get

  val schemaInspector = SchemaInspector(df)
  val categoricalVariables = schemaInspector.getCategoricalVariables
  val columnValues = cacheAnalysis.analyze(categoricalVariables)




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
