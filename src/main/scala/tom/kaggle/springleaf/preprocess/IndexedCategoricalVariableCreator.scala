package tom.kaggle.springleaf.preprocess

import java.io.{File, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import tom.kaggle.springleaf.{Names, SchemaInspector}

import scala.annotation.tailrec

case class IndexedCategoricalVariableCreator(df: DataFrame) {
  val categoricalVariables = SchemaInspector(df).getProcessedCategoricalVariables(df.schema)
  println(s"IndexedCategoricalVariableCreator of ${categoricalVariables.length} variables")

  lazy val models: Map[String, StringIndexerModel] =
    (for (v <- categoricalVariables) yield {
      val indexer = new StringIndexer()
        .setInputCol(v.name)
        .setOutputCol(s"${Names.PrefixOfIndexedString}_${v.name}")
      v.name -> indexer.fit(df)
    }).toMap

  lazy val transformedDf: DataFrame = transformFor(df, models.values.toList)

  @tailrec
  private def transform(df: DataFrame, models: List[StringIndexerModel]): DataFrame = {
    models match {
      case Nil => df
      case model :: rest => transform(model.transform(df), rest)
    }
  }

  private def transformFor(df: DataFrame, models: List[StringIndexerModel]): DataFrame = {
    models.foldLeft(df)((df: DataFrame, model: StringIndexerModel) => model.transform(df))
  }
}

object IndexedCategoricalVariableCreator {
  implicit val formats = org.json4s.DefaultFormats

  def readArray(path: String): Array[Int] = {
    val file = new File(path)
    parse(file).extract[Array[Int]]
  }

  def saveArray(models: Map[String, StringIndexerModel], path: String): Unit = {
    val numberOfLabels = models.map(model => model._2.labels.length)
    val prettyJson = pretty(seq2jvalue(numberOfLabels))
    storeIt(path, prettyJson)
  }

  def readMap(path: String): Map[String, Int] = {
    val file = new File(path)
    parse(file).extract[Map[String, Int]]
  }

  def saveMap(models: Map[String, StringIndexerModel], path: String): Unit = {
    val numberOfLabels: Map[String, Int] = models.map(model => model._1 -> model._2.labels.length)
    val prettyJson = pretty(map2jvalue(numberOfLabels))
    storeIt(path, prettyJson)
  }

  private def storeIt(path: String, content: String): Unit = {
    val file = new File(path)
    if (file.exists()) FileUtils.forceDelete(file)

    new PrintWriter(file) {
      write(content)
      close()
    }
  }
}