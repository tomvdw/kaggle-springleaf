package tom.kaggle.springleaf

import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.FileWriter
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

case class AnalysisReaderWriter(ac: ApplicationContext, analyzer: CategoricalColumnAnalyzer) extends ICachedAnalysis {
  private var writer: Option[PrintWriter] = None

  def readColumnValueCounts: Map[String, Map[String, Long]] = {
    try {
      val lines = scala.io.Source.fromFile(AnalysisReaderWriter.columnValueCountsPath).getLines()
      lines.map { line =>
        val parts = line.split(":", 2)
        val column = parts(0)
        val valueCounts = parts(1).split(";").map { v =>
          val p = v.substring(1, v.length() - 1).split(",")
          (p(0) -> p(1).toLong)
        }.toMap
        (column -> valueCounts)
      }.toMap
    } catch {
      case e: Throwable =>
        println("Could not read column value counts! " + e.toString())
        Map()
    }
  }

  def analyze(variables: Array[StructField]): Map[String, Map[String, Long]] = {
    val cachedColumnValueCounts = readColumnValueCounts
    val writer = getColumnValueCountsWriter
    for (variable <- variables) {
      if (!cachedColumnValueCounts.contains(variable.name)) {
        analyzeColumn(variable, writer)
      } else println("already analyzed " + variable.name)
    }
    writer.close
    readColumnValueCounts
  }

  private def analyzeColumn(column: StructField, writer: PrintWriter) {
    val valueCounts = analyzer.getValueCounts(ApplicationContext.tableName, column)
    writer.println("%s:%s".format(column.name, valueCounts.mkString(";")))
    writer.flush()
  }

  def getColumnValueCountsWriter: PrintWriter = {
    writer match {
      case Some(w) => w
      case None => {
        writer = Some(new PrintWriter(new BufferedWriter(new FileWriter(AnalysisReaderWriter.columnValueCountsPath, true))))
        writer.get
      }
    }
  }

  def close = {
    writer match {
      case Some(w) =>
        w.close
        writer = None
      case None => // Writer is already closed, so do nothing
    }
  }
}

object AnalysisReaderWriter {
  val columnValueCountsPath = ApplicationContext.dataFolderPath + "/column-value-counts" + ApplicationContext.fraction

  val valueRegex = "^\\(\\)$".r
}