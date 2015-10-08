package tom.kaggle.springleaf.analysis

case class ColumnValueAnalyzer(valueCounts: Map[String, Long], totalNumberOfRecords: Long) {

  lazy val max: Double = {
    doubleValueCounts.map { case (value, count) => value }.max
  }

  lazy val average: Double = {
    val total = doubleValueCounts.map { case (value, count) => value * count }.sum
    val numberOfValues = valueCounts.map { case (value, count) => count }.sum
    total / numberOfValues
  }

  lazy val std: Double = {
    val sumOfValuesMinusMeanSquared = doubleValueCounts
      .map { case (value, count) => count * scala.math.pow(value - average, 2) }.sum
    scala.math.sqrt(sumOfValuesMinusMeanSquared / totalNumberOfRecords)
  }

  lazy val doubleValueCounts: Map[Double, Long] = {
    valueCounts
      .filter { case (value, count) => !value.isEmpty }
      .map { case (value, count) => (value.toDouble, count) }
  }

}
