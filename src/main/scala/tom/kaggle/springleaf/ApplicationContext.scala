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

  val redisHost = "localhost"
  val redisPort = 6379
  val redis = new RedisClient(redisHost, redisPort)

  lazy val df = {
    val result = dataImporter.readSample
    result.registerTempTable(ApplicationContext.tableName)
    result
  }

  val schemaInspector = SchemaInspector(this)

  val analyzer = CategoricalColumnAnalyzer(this)
  val cachedAnalysis: ICachedAnalysis = RedisCacheAnalysis(this, analyzer)

}

object ApplicationContext {
  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val fraction = 0.01
  val tableName = "xxx"
}
