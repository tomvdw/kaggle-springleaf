package tom.kaggle.springleaf

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.redis.RedisClient
import tom.kaggle.springleaf.analysis.RedisCacheAnalysis
import tom.kaggle.springleaf.analysis.CategoricalColumnAnalyzer
import tom.kaggle.springleaf.analysis.ICachedAnalysis

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

  val redisHost = "localhost"
  val redisPort = 6379
  val redis = new RedisClient(redisHost, redisPort)

  lazy val df = {
    val result = dataImporter.readSample
    result.registerTempTable(ApplicationContext.tableName)
    result
  }

  val analyzer = CategoricalColumnAnalyzer(this)
  val cachedAnalysis: ICachedAnalysis = RedisCacheAnalysis(this, analyzer)

}

object ApplicationContext {
  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val fraction = 0.01
  val tableName = "xxx"
  val labelFieldName = "target"

  val integerRegex = "^-?\\d+$".r
  val doubleRegex = "^-?\\d+\\.\\d+$".r
  val dateRegex = "^\\d{2}[A-Z]{3}\\d{2}".r
  val booleanRegex = "^(false|true)$".r

}
