package tom.kaggle.springleaf.analysis

case class ColumnValueAnalyzer(valueCounts: Map[String, Long], totalNumberOfRecords: Long) {

  lazy val nulls: Long = valueCounts.count { case (value, count) => value.isEmpty }
  lazy val percentageNull: Double = nulls / totalNumberOfRecords

  lazy val twoBiggest: (Double, Double) = {
    if (doubleValues.size <= 1) (max, max)
    else {
      val secondBiggest = doubleValues.filter(x => x < max).max
      (max, secondBiggest)
    }
  }

  lazy val max: Double = doubleValues.max

  lazy val average: Double = {
    val total: Double = doubleValueCounts.map { case (value, count) => value * count }.sum
    val numberOfValues: Long = valueCounts.map { case (value, count) => count }.sum
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

  lazy val doubleValues: Iterable[Double] = {
    doubleValueCounts.map { case (value, count) => value }
  }

}
