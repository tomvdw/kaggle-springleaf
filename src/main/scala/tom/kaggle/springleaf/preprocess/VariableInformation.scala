package tom.kaggle.springleaf.preprocess

import tom.kaggle.springleaf.preprocess.ColumnTypeInference._

case class VariableInformation(name: String,
                               distinctValues: Long,
                               avg: String,
                               minimum: String,
                               maximum: String) {

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

  def hasOutliers: Boolean = {
    val m = Map("A" -> 1, "B" -> 2)
    val m2 = Map("A" -> Map("A1" -> 1, "A2" -> 2), "B" -> Map("B1" -> 2))
    val json = scala.util.parsing.json.JSONObject(m)
    ///scala.util.parsing.json.JSONObject.
    ???
  }

  private def minOrMaxIsNull: Boolean = VariableInformation.isNullValue(minimum) || VariableInformation.isNullValue(maximum)

  private def atLeastOne(test: String => Boolean): Boolean = {
    val minPasses = test(minimum)
    val maxPasses = test(maximum)
    if (minPasses && maxPasses) true
    else if ((minPasses || maxPasses) && minOrMaxIsNull) true
    else false
  }

  def areValuesNumeric: Boolean = atLeastOne(VariableInformation.isValueNumeric)

  def areValuesDates: Boolean = atLeastOne(VariableInformation.isValueDate)

  def areValuesBooleans: Boolean = atLeastOne(VariableInformation.isValueBoolean)

  override def toString = {
    val mininumValue: String = if (VariableInformation.isMaxOrMinValue(minimum)) "NULL" else minimum
    val maximumValue: String = if (VariableInformation.isMaxOrMinValue(maximum)) "NULL" else maximum
    s"Variable $name has $distinctValues distinct values, values are between $mininumValue and $maximumValue and avg is $avg"
  }
}

object VariableInformation {

  def isMaxOrMinValue(v: String): Boolean = Int.MaxValue.toString.equals(v) || Int.MinValue.toString.equals(v)

  def isValueNumeric(v: String): Boolean =
    !isMaxOrMinValue(v) && (IntegerRegex.findFirstIn(v).nonEmpty || DoubleRegex.findFirstIn(v).nonEmpty)

  def isValueDate(v: String): Boolean = DateRegex.findFirstIn(v).nonEmpty

  def isValueBoolean(v: String): Boolean = BooleanRegex.findFirstIn(v).nonEmpty


  def isNullValue(v: String): Boolean = "null".equals(v) || v.isEmpty

}