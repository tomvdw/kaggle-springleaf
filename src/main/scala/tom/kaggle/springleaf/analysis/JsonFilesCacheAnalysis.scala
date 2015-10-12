package tom.kaggle.springleaf.analysis

import java.io.{File, PrintWriter}

import org.apache.spark.sql.types.StructField
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class JsonFilesCacheAnalysis(columnValueCountsPath: String, statistics: IDataStatistics) extends ICachedAnalysis {
  implicit val formats = org.json4s.DefaultFormats

  override def readColumnValueCounts: Map[String, Map[String, Long]] = {
    val columnValueCountsFolder = new File(columnValueCountsPath)
    columnValueCountsFolder.mkdirs()
    try {
      val variableFiles = columnValueCountsFolder.listFiles().filter(f => f.getName.startsWith("VAR_"))
      variableFiles.map(f => {
        val variableName = f.getName
        variableName -> parse(f).extract[Map[String, Long]]
      }).toMap
    } catch {
      case e: Throwable => Map()
    }
  }

  override def analyze(variables: Array[StructField]): Map[String, Map[String, Long]] = {
    val cachedColumnValueCounts = readColumnValueCounts
    for (variable <- variables) {
      if (!cachedColumnValueCounts.contains(variable.name)) {
        println(s"Analyzing value counts of ${variable.name}")
        val valueCounts = statistics.valueCount(variable).toMap
        val prettyJson = pretty(map2jvalue(valueCounts))
        new PrintWriter(s"$columnValueCountsPath/${variable.name}") {
          write(prettyJson); close
        }
      } //else println("already analyzed " + variable.name)
    }
    readColumnValueCounts
  }
}
