package tom.kaggle.springleaf.preprocess

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, StringType}

case class OnTheFlyTypeInferrer(df: DataFrame) extends ITypeInferrer {

  override def inferTypes: Map[String, DataType] = {
    val allVariables = df.schema.fields.filter(f => f.name.startsWith("VAR_")).map(v => v.name)
    val metaData: Array[VariableInformation] = getMetaData(allVariables)
    metaData.foreach(println)

    ???
  }


  private def getMetaData(allVariables: Array[String]): Array[VariableInformation] = {
    val selectExpressions = allVariables.flatMap(variable => {
      val c = col(variable)
      val whenVariableIsNull = s"when $variable is null or $variable = 'null' or $variable = ''"
      List(
        approxCountDistinct(c).as(s"DISTINCT_$variable"),
        avg(c.cast(DoubleType)).cast(StringType).as(s"AVG_$variable"),
        min(expr(s"case $whenVariableIsNull then ${Int.MaxValue} else $variable end")).cast(StringType).as(s"MIN_$variable"),
        max(expr(s"case $whenVariableIsNull then ${Int.MinValue} else $variable end")).cast(StringType).as(s"MAX_$variable"))
    })
    val result = df.agg(selectExpressions.head, selectExpressions.tail: _*).collect().apply(0)
    allVariables.map(variable =>
      VariableInformation(
        variable,
        result.getAs[Long](s"DISTINCT_$variable"),
        result.getAs[String](s"AVG_$variable"),
        result.getAs[String](s"MIN_$variable"),
        result.getAs[String](s"MAX_$variable")))
  }
}
