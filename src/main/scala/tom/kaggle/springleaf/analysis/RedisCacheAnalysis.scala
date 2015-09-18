package tom.kaggle.springleaf.analysis

import com.redis.serialization.Parse.Implicits.{parseLong, parseString}
import org.apache.spark.sql.types.StructField
import tom.kaggle.springleaf.{ApplicationContext, KeyHelper}

case class RedisCacheAnalysis(ac: ApplicationContext, analyzer: CategoricalColumnAnalyzer) extends ICachedAnalysis {
  val keyHelper = KeyHelper(ac)

  def readColumnValueCounts: Map[String, Map[String, Long]] = {
    getValueCountsPerKey.map {
      case (key, value) => (keyHelper.nameOf(key), value)
    }
  }

  private def getValueCountsPerKey: Map[String, Map[String, Long]] = {
    getCachedVariables.map { key =>
      ac.redis.hgetall[String, Long](key) match {
        case Some(values) => key -> values
        case None         => throw new RuntimeException("No values found for %s!!!".format(key))
      }
    }.toMap
  }

  private def getCachedVariables: List[String] = {
    ac.redis.keys[String](keyHelper.keyPattern) match {
      case Some(keys) => keys.flatMap { x => x }
      case None       => List()
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
    if (!ac.redis.exists(columnKey)) {
      val valueCounts = analyzer.getValueCounts(ApplicationContext.tableName, column)
      valueCounts.foreach(valueCount => ac.redis.hset(columnKey, valueCount._1, valueCount._2))
    }
  }

}