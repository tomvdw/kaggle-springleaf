package tom.kaggle.springleaf.analysis

case class ColumnValueAnalyzer(valueCounts: Map[String, Long], totalNumberOfRecords: Long) {

  lazy val sortedByCount: Seq[(String, Long)] = valueCounts.toSeq.sortBy(_._2)
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

  lazy val doubleValues: Iterable[Double] =
    doubleValueCounts.map { case (value, count) => value }

  lazy val sortedDoubleValueCounts: Seq[(Double, Long)] =
    doubleValueCounts.toSeq.sortBy(_._1)

  lazy val percentile5 = percentile(5)
  lazy val percentile95 = percentile(95)

  def percentile(tile: Int): Double = {
    val totalOccurrences = sortedDoubleValueCounts.map(_._2).sum
    val desired = totalOccurrences * (tile / 100d)
    var current: Long = 0
    for ((value, occurrences) <- sortedDoubleValueCounts) {
      current = current + occurrences
      if (current >= desired) return value
    }
    sortedDoubleValueCounts.last._1
  }

}
