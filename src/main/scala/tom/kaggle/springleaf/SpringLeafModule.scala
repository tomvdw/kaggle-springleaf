package tom.kaggle.springleaf

import com.redis.RedisClient
import scaldi.Module
import tom.kaggle.springleaf.analysis._
import tom.kaggle.springleaf.preprocess.{DataImporter, CategoricToIndexTransformer, DataPreProcessor}

class SpringLeafModule extends Module {

  binding to new RedisClient(
    host = inject[String]("redis.host"),
    port = inject[Int]("redis.port")
  )
  binding to injected[KeyHelper]('fraction -> inject[Double]("data.fraction"))
  binding to injected[DataStatistics]('table -> Names.TableName)
  bind [ICachedAnalysis] to injected[JsonFilesCacheAnalysis]('columnValueCountsPath -> inject[String]("data.path.columnValueCounts"), 'statistics -> inject[DataStatistics])
  binding to new ColumnTypeInference
  binding to injected[DataImporter]('dataPath -> inject[String]("data.path.base"), 'fraction -> inject[Double]("data.fraction"))
  binding identifiedBy "everything" to {
    val result = inject[DataImporter].readSample
    result.registerTempTable(Names.TableName)
    result
  }
  binding to injected[DataPreProcessor]
  binding to injected[CategoricToIndexTransformer]('dataPath -> inject[String]("data.path.base"), 'fraction -> inject[Double]("data.fraction"))
}
