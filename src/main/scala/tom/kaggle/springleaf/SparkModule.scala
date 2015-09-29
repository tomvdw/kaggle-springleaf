package tom.kaggle.springleaf

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scaldi.Module

class SparkModule extends Module {

  import SparkModule._

  bind[SparkContext] to {
    val conf = new SparkConf()
      .setAppName("Kaggle SpringLeaf")
      .setMaster("local[*]")
      .set("spark.executor.memory", "8g")
      .set("spark.driver.memory", "8g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val context = new SparkContext(conf)
    context.hadoopConfiguration.setInt("parquet.block.size", SixtyFourMegabytes)
    context
  }

  bind[SQLContext] to new SQLContext(inject[SparkContext])
}

object SparkModule {
  val SixtyFourMegabytes = 64 * 1024 * 1024
}
