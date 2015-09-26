package tom.kaggle.springleaf.analysis

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import tom.kaggle.springleaf.SchemaInspector

case class RedisTypeInferrer(cachedInferredTypesPath: String,
                             df: DataFrame,
                             cacheAnalysis: ICachedAnalysis,
                             columnTypeInferrer: ColumnTypeInference) extends ITypeInferrer {

  override def inferTypes: Map[String, DataType] = {
    val schemaInspector = SchemaInspector(df)
    readInferredTypes().getOrElse {
      val categoricalVariables = schemaInspector.getCategoricalVariables
      println(s"${categoricalVariables.length} number of categoricalVariables")
      val columnValues = cacheAnalysis.analyze(categoricalVariables)
      val inferredTypes = columnTypeInferrer.inferTypes(columnValues)
      inferredTypes.foreach {
        case (column, dataType) => println(s"Inferred type ${dataType.typeName} for column $column")
      }
      saveInferredTypes(inferredTypes)
      inferredTypes
    }
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
