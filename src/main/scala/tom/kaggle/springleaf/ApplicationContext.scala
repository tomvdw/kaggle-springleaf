package tom.kaggle.springleaf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class ApplicationContext {
  val conf = new SparkConf()
    .setAppName("Kaggle Springleaf")
    .setMaster("local[*]")
    .set("spark.executor.memory", "8g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf)
  val ONE_GB = 1024 * 1024 * 1024
  sc.hadoopConfiguration.setInt("parquet.block.size", ONE_GB)

  val sqlContext = new SQLContext(sc)
  val dataImporter = new DataImporter(sc, sqlContext)

}