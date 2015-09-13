package tom.kaggle.springleaf

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import java.io.PrintWriter
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

case class CategoricToIndexTransformer(
    ac: ApplicationContext,
    newColumnPrefix: String = "ind_",
    outputFileName: String = "string-index-output" + ApplicationContext.fraction) {

  private def debug(model: StringIndexerModel, columnName: String, writer: PrintWriter) {
    println("Processed column %s with %d different values".format(columnName, model.labels.size))
    writer.println("%s:%s".format(columnName, model.labels.mkString("'", "','", "'")))
    writer.flush()
  }

  def transform: (DataFrame, Map[String, Array[String]]) = {
    val writer = new PrintWriter(ApplicationContext.dataFolderPath + "/" + outputFileName, "UTF-8")

    var tmpDf = ac.df
    var indexedNames: Map[String, Array[String]] = Map()
    for (v <- ac.schemaInspector.getCategoricalVariables) {
      val indexedName = newColumnPrefix + v.name
      val indexer = new StringIndexer().setInputCol(v.name).setOutputCol(indexedName)
      val model = indexer.fit(ac.df)
      debug(model, v.name, writer)
      tmpDf = model.transform(tmpDf)
      indexedNames.put(indexedName, model.labels)
    }
    writer.close()
    (tmpDf, indexedNames)
  }

}