package tom.kaggle.springleaf

import com.redis.RedisClient
import scaldi.Module
import tom.kaggle.springleaf.analysis.{ColumnTypeInference, DataStatistics, RedisCacheAnalysis}

class SpringLeafModule extends Module {
  private val fraction = inject[Double]("data.fraction")
  private val dataPath = inject[String]("data.path.base")

  binding to new RedisClient(
    host = inject[String]("redis.host"),
    port = inject[Int]("redis.port")
  )
  binding to injected[KeyHelper]('fraction -> fraction)
  binding to injected[RedisCacheAnalysis]
  binding to injected[DataStatistics]('table -> Names.TableName)
  binding to new ColumnTypeInference
  binding to injected[DataImporter]('dataPath -> dataPath, 'fraction -> fraction)
  binding identifiedBy "everything" to {
    val result = inject[DataImporter].readSample
    result.registerTempTable(Names.TableName)
    result
  }
  binding to injected[DataPreProcessor]
  binding to injected[CategoricToIndexTransformer]('dataPath -> dataPath, 'fraction -> fraction)
}
