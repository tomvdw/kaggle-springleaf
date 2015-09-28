package tom.kaggle.springleaf.preprocess

import org.apache.spark.sql.types.DataType

trait ITypeInferrer {

  def inferTypes: Map[String, DataType]

}
