package tom.kaggle.springleaf.preprocess

import java.io.PrintWriter

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
import tom.kaggle.springleaf.SchemaInspector

import scala.collection.{immutable, mutable}

case class CategoricToIndexTransformer(df: DataFrame, fraction: Double, dataPath: String, newColumnPrefix: String = "ind_") {

  val outputFileName: String = s"string-index-output$fraction"

  private def debug(model: StringIndexerModel, columnName: String, writer: PrintWriter) {
    println(s"Processed column $columnName with ${model.labels.length} different values")
    writer.println(s"$columnName:${model.labels.mkString("'", "','", "'")}")
    writer.flush()
  }

  def transform: (DataFrame, immutable.Map[String, Array[String]]) = {
    val writer = new PrintWriter(s"$dataPath/$outputFileName", "UTF-8")

    var tmpDf = df
    val indexedNames: mutable.Map[String, Array[String]] = mutable.Map()
    for (v <- SchemaInspector(df).getCategoricalVariables) {
      val indexedName = newColumnPrefix + v.name
      val indexer = new StringIndexer().setInputCol(v.name).setOutputCol(indexedName)
      val model = indexer.fit(df)
      debug(model, v.name, writer)
      tmpDf = model.transform(tmpDf)
      indexedNames.put(indexedName, model.labels)
    }
    writer.close()
    (tmpDf, indexedNames.toMap)
  }

}
