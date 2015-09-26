package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.DataType

trait ITypeInferrer {

  def inferTypes: Map[String, DataType]

}
