package tom.kaggle.springleaf

import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.FileWriter

case class AnalysisReaderWriter() {
  private var writer: Option[PrintWriter] = None

  def readColumnValueCounts: Map[String, Array[(String, Long)]] = {
    try {
      val lines = scala.io.Source.fromFile(AnalysisReaderWriter.columnValueCountsPath).getLines()
      lines.map { line =>
        val parts = line.split(":", 2)
        val column = parts(0)
        val valueCounts = parts(1).split(";").map { v =>
          val p = v.substring(1, v.length() - 1).split(",")
          (p(0) -> p(1).toLong)
        }
        (column -> valueCounts)
      }.toMap
    } catch {
      case e: Throwable =>
        Map()
    }
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
    }
  }
}

object AnalysisReaderWriter {
  val columnValueCountsPath = ApplicationContext.dataFolderPath + "/column-value-counts" + ApplicationContext.fraction

  val valueRegex = "^\\(\\)$".r
}