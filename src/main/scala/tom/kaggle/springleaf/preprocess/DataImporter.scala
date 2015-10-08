package tom.kaggle.springleaf.preprocess

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataImporter(dataPath: String, fraction: Double, sc: SparkContext, sqlContext: SQLContext) {

  val csvTrainPath = dataPath + "trainMissing.csv"
  val sampledCsvTrainPath = dataPath + "trainMissing.sampled"
  val sampledParquetTrainPath = dataPath + "trainMissing.sampled"
  val dfTrainPath = dataPath + "train.parquet"
  val jsonTrainPath = dataPath + "train.json"
  val rddTrainPath = dataPath + "train.rdd"

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

  def readParquetFile: DataFrame = {
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
//      .option("inferSchema", "true")
      .load(filePath).repartition(256)

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
    //val path = sampledCsvTrainPath + "." + fraction
    val path = sampledParquetTrainPath + "." + fraction
    try {
      //readCsv(path)
      readParquetFile
    } catch {
      case e: Throwable =>
        println("Could not read parquet")
        val df = readCsv
        val sampledDf = if (fraction < 1.0) df.sample(withReplacement = false, fraction) else df
        sampledDf.write
          .format(databricksCsvPackage)
          .option("header", "true")
          .save(path)
        sampledDf
    }
  }
}
