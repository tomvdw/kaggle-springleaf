package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
//import scalaz._
//import Scalaz._

class DataStatistics(sqlContext: SQLContext, table: String) {

  def valueCount(column: StructField): Array[(String, Long)] =
    sqlContext.sql(s"select ${column.name}, count(1) from $table group by ${column.name}")
      .map(r => r.getString(0) -> r.getLong(1))
      .collect()

//  type ValueMap = mutable.Map[String, mutable.Map[Any, Int]]
//  type ImmutableValueMap = Map[String, Map[Any, Int]]

  def tmp(): mutable.Map[String, mutable.Map[Any, Int]] = {
    val tableDf = sqlContext.table(table)
    tableDf.describe().show(10)
    val fieldsWithIndex = tableDf.schema.fieldNames.zipWithIndex.filter { case (name, index) => name.startsWith("VAR_") }

//    val xxx = tableDf.schema.fieldNames.filter(_.startsWith("VAR_"))
//    tableDf.cube(xxx(1), xxx(2)).

/*
    val zeroValue: mutable.Map[String, mutable.Map[Any, Int]] = mutable.Map()
    val aggregatedValues = tableDf.rdd.aggregate(zeroValue)(seqOp(fieldsWithIndex)_, combOp(fieldsWithIndex)_)
    aggregatedValues
*/
    mutable.Map()

//    val seqOp = immseqOp(fieldsWithIndex)_
//    val zeroValueMap: Map[String, Map[Any, Int]] = Map()
//    tableDf.rdd.aggregate(zeroValueMap)(seqOp, immcombOp)
  }
/*

  private def immseqOp(fieldsWithIndex: Array[(String, Int)])(soFar: Map[String, Map[Any, Int]], next: Row): Map[String, Map[Any, Int]] = {
    val values = for ((name, index) <- fieldsWithIndex) yield {
      val variableSoFar = soFar.getOrElse(name, Map[Any, Int]())
      val value: Any = next.get(index)
      val occurrences = variableSoFar.getOrElse(value, 0)
      name -> (variableSoFar + (value -> (occurrences + 1)))
    }
    values.toMap
  }

  private def immcombOp(x: Map[String, Map[Any, Int]], y: Map[String, Map[Any, Int]]): Map[String, Map[Any, Int]] = {
    (for (key <- x.keySet ++ y.keySet) yield {
      key -> (x.getOrElse(key, Map()) |+| y.getOrElse(key, Map()))
    }).toMap
  }
*/

  private def seqOp(fieldsWithIndex: Array[(String, Int)])(soFar: mutable.Map[String, mutable.Map[Any, Int]], next: Row): mutable.Map[String, mutable.Map[Any, Int]] = {
    for ((name, index) <- fieldsWithIndex) {
      val variableSoFar = soFar.getOrElse(name, mutable.Map())
      val value: Any = next.get(index)
      val occurrences = variableSoFar.getOrElseUpdate(value, 0)
      variableSoFar.put(value, occurrences + 1)
    }
    soFar
  }

  private def combOp(fieldsWithIndex: Array[(String, Int)])(x: mutable.Map[String, mutable.Map[Any, Int]], y: mutable.Map[String, mutable.Map[Any, Int]]): mutable.Map[String, mutable.Map[Any, Int]] = {
    val result: mutable.Map[String, mutable.Map[Any, Int]] = mutable.Map()
    for ((name, index) <- fieldsWithIndex) {
      val xValueMap = x.getOrElse(name, mutable.Map())
      val yValueMap = y.getOrElse(name, mutable.Map())
      val combinedValueMap: mutable.Map[Any, Int] = mutable.Map()
      for (key <- xValueMap.keySet.union(yValueMap.keySet)) {
        combinedValueMap.put(key, xValueMap.getOrElse(key, 0) + yValueMap.getOrElse(key, 0))
      }
      result.put(name, combinedValueMap)
    }
    result
  }


}
