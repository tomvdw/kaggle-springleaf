package tom.kaggle.springleaf

import org.apache.spark.sql.SQLContext

class DataImporter(sqlContext: SQLContext) {

  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val csvTrainPath = dataFolderPath + "train.csv"
  val dfTrainPath = dataFolderPath + "dfTrain.parquet"
  val jsonTrainPath = dataFolderPath + "jsonTrain.json"

  def readJson = {
    try {
      sqlContext.read.json(jsonTrainPath)
    } catch {
      case e: Throwable =>
        val df = readCsv
        df.write.json(jsonTrainPath)
        df
    }
  }

  def readParquet = {
    try {
      sqlContext.read.parquet(dfTrainPath)
    } catch {
      case e: Throwable =>
        val df = readCsv
        df.write.parquet(dfTrainPath)
        df
    }
  }

  def readCsv = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csvTrainPath)

}