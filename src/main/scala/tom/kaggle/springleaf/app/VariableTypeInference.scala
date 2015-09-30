package tom.kaggle.springleaf.app

import org.apache.spark.sql.types._

class VariableTypeInference(totalNumberOfRecords: Long) {

  def getVariableTypes(columnTypes: Map[String, DataType], valuesPerColumn: Map[String, Map[String, Long]]): Map[String, DataType] = {
    println(s"Determining variable types of ${columnTypes.size} column types / ${valuesPerColumn.size} values per column")
    columnTypes.map {
      case (variable, dataType) =>
        val newDataType = getVariableType(variable, dataType, valuesPerColumn.get(variable).get)
        if (dataType != newDataType) println(s"Changed $variable from $dataType to $newDataType")
        variable -> newDataType
    }
  }

  def getVariableType(variable: String, dataType: DataType, valueCounts: Map[String, Long]): DataType = {
    dataType match {
      case DoubleType | LongType | IntegerType =>
        doubleDataType(variable, valueCounts)
      case _ => dataType
    }
  }

  def doubleDataType(variable: String, valueCounts: Map[String, Long]): DataType = {
    val numberOfDistinctValues: Int = valueCounts.size
    val hasVeryFewDistinctValues: Boolean = numberOfDistinctValues < 5
    val hasFewDistinctValues: Boolean = numberOfDistinctValues < 50
    println(s"$variable has ${numberOfDistinctValues}")

    // Less than 5 distinct values, so probably this is categorical
    if (hasVeryFewDistinctValues) {
      println(s"$variable has very few distinct values (=${numberOfDistinctValues}) => StringType")
      return StringType
    }

    val numberOfNullValues: Long = valueCounts.getOrElse("", 0)
    val hasMostlyNullValues: Boolean = numberOfNullValues / totalNumberOfRecords > 0.9 // More than 90% of the values are null

    // >90% null values, so to use it, we'd need to impute, but then how useful is this column?
    if (hasMostlyNullValues) {
      println(s"$variable has mostly null values (=${numberOfNullValues}) => StringType")
      return StringType
    }

    val avgValue = average(valueCounts)
    val maxValue = max(valueCounts)
    val hasOutlier: Boolean = maxValue / avgValue > 10 // has outlier iff the max is more than 10 times the average value

    // if there is an outlier, and there are not a lot of distinct values, then just make it categorical...
    if (hasFewDistinctValues && hasOutlier) {
      println(s"$variable has an outlier (=$maxValue, avg=$avgValue) and few distinct values (=${numberOfDistinctValues}) => StringType")
      return StringType
    }

    DoubleType
  }

  def averages(valuesPerColumn: Map[String, Map[String, Long]]): Map[String, Double] = {
    valuesPerColumn.map { case (variable, valueCounts) => variable -> average(valueCounts) }
  }

  def max(valueCounts: Map[String, Long]): Double = {
    getDoubles(valueCounts).map { case (value, count) => value }.max
  }

  def average(valueCounts: Map[String, Long]): Double = {
    val total = getDoubles(valueCounts).map { case (value, count) => value * count }.sum
    val numberOfValues = valueCounts.map { case (value, count) => count }.sum
    total / numberOfValues
  }

  def std(valueCounts: Map[String, Long]): Double = {
    val avg = average(valueCounts)
    val sumOfValuesMinusMeanSquared = getDoubles(valueCounts)
      .map { case (value, count) => count * scala.math.pow(value - avg, 2) }.sum
    scala.math.sqrt(sumOfValuesMinusMeanSquared / totalNumberOfRecords)
  }

  def getDoubles(valueCounts: Map[String, Long]): Map[Double, Long] = {
    valueCounts
      .filter { case (value, count) => !value.isEmpty }
      .map { case (value, count) => (value.toDouble, count) }
  }

}
