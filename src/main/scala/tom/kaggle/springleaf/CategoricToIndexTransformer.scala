package tom.kaggle.springleaf

import java.io.PrintWriter

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame

import scala.collection.{immutable, mutable}

case class CategoricToIndexTransformer(ac: ApplicationContext, newColumnPrefix: String = "ind_") {

  val outputFileName: String = s"string-index-output${ac.fraction}"

  private def debug(model: StringIndexerModel, columnName: String, writer: PrintWriter) {
    println(s"Processed column $columnName with ${model.labels.length} different values")
    writer.println(s"$columnName:${model.labels.mkString("'", "','", "'")}")
    writer.flush()
  }

  def transform: (DataFrame, immutable.Map[String, Array[String]]) = {
    val writer = new PrintWriter(s"${ac.dataFolderPath}/$outputFileName", "UTF-8")

    var tmpDf = ac.df
    val indexedNames: mutable.Map[String, Array[String]] = mutable.Map()
    for (v <- SchemaInspector(ac.df).getCategoricalVariables) {
      val indexedName = newColumnPrefix + v.name
      val indexer = new StringIndexer().setInputCol(v.name).setOutputCol(indexedName)
      val model = indexer.fit(ac.df)
      debug(model, v.name, writer)
      tmpDf = model.transform(tmpDf)
      indexedNames.put(indexedName, model.labels)
    }
    writer.close()
    (tmpDf, indexedNames.toMap)
  }

}