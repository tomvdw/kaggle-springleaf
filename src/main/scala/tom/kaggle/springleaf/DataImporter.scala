package tom.kaggle.springleaf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class DataImporter(sc: SparkContext, sqlContext: SQLContext) {
  import sqlContext.implicits._

  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val csvTrainPath = dataFolderPath + "trainMissing.csv"
  val dfTrainPath = dataFolderPath + "dfTrain.parquet"
  val jsonTrainPath = dataFolderPath + "jsonTrain.json"
  val rddTrainPath = dataFolderPath + "rddTrain.rdd"

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

  def readRdd = {
    try {
      val rdd = sc.objectFile(rddTrainPath, 16)
      rdd
    } catch {
      case e: Throwable =>
        val df = readCsv
        df.rdd.saveAsObjectFile(rddTrainPath)
        df.rdd
    }
  }

}