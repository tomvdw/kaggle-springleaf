package tom.kaggle.springleaf.analysis

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits.{parseLong, parseString}
import org.apache.spark.sql.types.StructField
import tom.kaggle.springleaf.KeyHelper

class RedisCacheAnalysis(redis: RedisClient, statistics: DataStatistics, keyHelper: KeyHelper) extends ICachedAnalysis {

  def readColumnValueCounts: Map[String, Map[String, Long]] = {
    getValueCountsPerKey.map {
      case (key, value) => (keyHelper.nameOf(key), value)
    }
  }

  private def getValueCountsPerKey: Map[String, Map[String, Long]] = {
    getCachedVariables.map { key =>
      redis.hgetall[String, Long](key) match {
        case Some(values) => key -> values
        case None => throw new RuntimeException(s"No values found for $key!!!")
      }
    }.toMap
  }

  private def getCachedVariables: List[String] = {
    redis.keys[String](keyHelper.keyPattern) match {
      case Some(keys) => keys.flatten
      case None => List()
    }
  }

  def analyze(variables: Array[StructField]): Map[String, Map[String, Long]] = {
    val cachedColumnValueCounts = readColumnValueCounts
    for (variable <- variables if !cachedColumnValueCounts.contains(variable.name)) {
      analyzeColumn(variable)
    }
    readColumnValueCounts
  }

  private def analyzeColumn(column: StructField) {
    val columnKey = keyHelper.keyFor(column)
    if (!redis.exists(columnKey)) {
      statistics.valueCount(column)
        .foreach(valueCount => redis.hset(columnKey, valueCount._1, valueCount._2))
    }
  }
}
