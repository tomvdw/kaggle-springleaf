package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StringType}
import tom.kaggle.springleaf.analysis.ColumnTypeInference._

case class OnTheFlyTypeInferrer(df: DataFrame) extends ITypeInferrer {

  private case class VariableInformation(name: String, distinctValues: Long, avg: String, minimum: String, maximum: String) {

    def shouldBeIgnored: Boolean = distinctValues <= 1

    def shouldBeCategorical(totalNumberOfRows: Int): Boolean = {
      if (minOrMaxIsNull) {
        if (areValuesNumeric) {
          if (distinctValues < 10) true
          else false
        }
        else true
      } else {

        false
      }
    }

    private def isNullValue(v: String): Boolean = "null".equals(v) || v.isEmpty

    private def minOrMaxIsNull: Boolean = isNullValue(minimum) || isNullValue(maximum)

    private def atLeastOne(test: String => Boolean): Boolean = {
      val minPasses = test(minimum)
      val maxPasses = test(maximum)
      if (minPasses && maxPasses) true
      else if ((minPasses || maxPasses) && minOrMaxIsNull) true
      else false
    }

    def areValuesNumeric: Boolean = {
      def isValueNumeric(v: String): Boolean = IntegerRegex.findFirstIn(v).nonEmpty || DoubleRegex.findFirstIn(v).nonEmpty
      atLeastOne(isValueNumeric)
    }

    def areValuesDates: Boolean = {
      def isValueDate(v: String): Boolean = DateRegex.findFirstIn(v).nonEmpty
      atLeastOne(isValueDate)
    }

    def areValuesBooleans: Boolean = {
      def isValueBoolean(v: String): Boolean = BooleanRegex.findFirstIn(v).nonEmpty
      atLeastOne(isValueBoolean)
    }

    override def toString =
      s"Variable $name has $distinctValues distinct values, values are between $minimum and $maximum and avg is $avg"
  }

  override def inferTypes: Map[String, DataType] = {
    val allVariables = df.schema.fields.filter(f => f.name.startsWith("VAR_")).map(v => v.name)
    val metaData: Array[VariableInformation] = getMetaData(allVariables)
    metaData.foreach(println)

    ???
  }


  private def getMetaData(allVariables: Array[String]): Array[VariableInformation] = {
    val selectExpressions = allVariables.flatMap(variable => List(
      approxCountDistinct(variable).as(s"DISTINCT_$variable"),
      avg(variable).cast(StringType).as(s"AVG_$variable"),
      min(variable).cast(StringType).as(s"MIN_$variable"),
      max(variable).cast(StringType).as(s"MAX_$variable")))
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
