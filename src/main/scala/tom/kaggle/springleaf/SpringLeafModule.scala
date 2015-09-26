package tom.kaggle.springleaf

import com.redis.RedisClient
import org.apache.spark.sql.DataFrame
import scaldi.Module
import tom.kaggle.springleaf.analysis._

class SpringLeafModule extends Module {

  /*binding to new RedisClient(
    host = inject[String]("redis.host"),
    port = inject[Int]("redis.port")
  )
  binding to injected[KeyHelper]('fraction -> inject[Double]("data.fraction"))
  binding to injected[RedisCacheAnalysis]
  */
  bind [ICachedAnalysis] to injected[OnTheFlyCachedAnalysis]('table -> Names.TableName)
  binding to injected[DataStatistics]('table -> Names.TableName)
  binding to new ColumnTypeInference
  binding to injected[DataImporter]('dataPath -> inject[String]("data.path.base"), 'fraction -> inject[Double]("data.fraction"))
  binding identifiedBy "everything" to {
    val result = inject[DataImporter].readSample
    result.registerTempTable(Names.TableName)
    result
  }
  binding to injected[DataPreProcessor]
  binding to injected[CategoricToIndexTransformer]('dataPath -> inject[String]("data.path.base"), 'fraction -> inject[Double]("data.fraction"))
/*
  bind [ITypeInferrer] to injected[RedisTypeInferrer](
    'cachedInferredTypesPath -> inject[String]("data.path.cachedInferredTypes"),
    'df -> inject[DataFrame],
    'cacheAnalysis -> inject[ICachedAnalysis],
    'columnTypeInferrer -> inject[ColumnTypeInference])
*/

  bind [ITypeInferrer] to injected[OnTheFlyTypeInferrer]('df -> inject[DataFrame])

}
