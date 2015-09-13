package tom.kaggle.springleaf

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class DataImporter(sc: SparkContext, sqlContext: SQLContext) {
  import sqlContext.implicits._

  val csvTrainPath = ApplicationContext.dataFolderPath + "trainMissing.csv"
  val sampledCsvTrainPath = ApplicationContext.dataFolderPath + "trainMissing.sampled"
  val dfTrainPath = ApplicationContext.dataFolderPath + "train.parquet"
  val jsonTrainPath = ApplicationContext.dataFolderPath + "train.json"
  val rddTrainPath = ApplicationContext.dataFolderPath + "train.rdd"

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
    val path = sampledCsvTrainPath + "." + ApplicationContext.fraction
    try {
      readCsv(path)
    } catch {
      case e: Throwable =>
        val df = readCsv
        val sampledDf = df.sample(false, ApplicationContext.fraction)
        sampledDf.write
          .format(databricksCsvPackage)
          .option("header", "true")
          .save(path)
        sampledDf
    }
  }
}

object DataImporter {

}