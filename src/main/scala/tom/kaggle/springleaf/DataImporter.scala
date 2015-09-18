package tom.kaggle.springleaf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataImporter(dataFolderPath: String, fraction: Double, sc: SparkContext, sqlContext: SQLContext) {

  val csvTrainPath = dataFolderPath + "trainMissing.csv"
  val sampledCsvTrainPath = dataFolderPath + "trainMissing.sampled"
  val dfTrainPath = dataFolderPath + "train.parquet"
  val jsonTrainPath = dataFolderPath + "train.json"
  val rddTrainPath = dataFolderPath + "train.rdd"

  val databricksCsvPackage = "com.databricks.spark.csv"

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

  def readCsv: DataFrame = readCsv(csvTrainPath)

  def readCsv(filePath: String): DataFrame =
    sqlContext.read.format(databricksCsvPackage)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)

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

  def readSample: DataFrame = {
    val path = sampledCsvTrainPath + "." + fraction
    try {
      readCsv(path)
    } catch {
      case e: Throwable =>
        val df = readCsv
        val sampledDf = df.sample(withReplacement = false, fraction)
        sampledDf.write
          .format(databricksCsvPackage)
          .option("header", "true")
          .save(path)
        sampledDf
    }
  }
}
