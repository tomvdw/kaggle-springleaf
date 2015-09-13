package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructType
import java.io.PrintWriter
import org.apache.spark.sql.types.StructField

object AnalyzeDataApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext

    val tableName = "xxx"
    val df = ac.dataImporter.readSample
    df.registerTempTable(tableName)

    val schemaInspector = SchemaInspector(df)
    val variables = schemaInspector.getCategoricalVariables

    val analyzer = CategoricalColumnAnalyzer(ac, df)
    val analysisReaderWriter = AnalysisReaderWriter()
    val readColumnValueCounts = analysisReaderWriter.readColumnValueCounts

    val writer = analysisReaderWriter.getColumnValueCountsWriter
    for (variable <- variables) {
      if (!readColumnValueCounts.contains(variable.name)) {
        analyzeColumn(variable, writer)
      } else println("already analyzed " + variable.name)
    }
    analysisReaderWriter.close

    def analyzeColumn(column: StructField, writer: PrintWriter) {
      val valueCounts = analyzer.getValueCounts(tableName, column)
      writer.println("%s:%s".format(column.name, valueCounts.mkString(";")))
      writer.flush()
      if (!ac.redis.exists(column.name)) {
    	  valueCounts.foreach(valueCount => ac.redis.hset(createKey(column), valueCount._1, valueCount._2))
      }
    }
    
  }
  
  def createKey(column: StructField): String = {
    List("column", column.name, ApplicationContext.fraction).mkString(":")
  }
}