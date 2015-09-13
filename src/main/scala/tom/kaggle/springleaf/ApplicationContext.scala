package tom.kaggle.springleaf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.redis.RedisClient

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

  val redis = new RedisClient("localhost", 6379)

}

object ApplicationContext {
  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val fraction = 0.01
}
