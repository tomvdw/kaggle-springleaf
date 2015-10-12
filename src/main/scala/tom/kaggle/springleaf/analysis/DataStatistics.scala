package tom.kaggle.springleaf.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField

class DataStatistics(sqlContext: SQLContext, table: String) extends IDataStatistics {

  override def valueCount(column: StructField): Array[(String, Long)] = {
    sqlContext.table(table).select(column.name).map(_.getString(0)).countByValue().toArray
    /*
        sqlContext.sql(s"select ${column.name}, count(1) from $table group by ${column.name}")
          .map(r => r.getString(0) -> r.getLong(1))
          .collect()
    */
  }

  /*
    def tmp(): Map[String, Map[Any, Int]] = {
      val tableDf = sqlContext.table(table)
      tableDf.describe().show(10)
      val fieldsWithIndex = tableDf.schema.fieldNames.zipWithIndex.filter { case (name, index) => name.startsWith("VAR_") }

      val seqOp = immseqOp(fieldsWithIndex) _
      val zeroValueMap: Map[String, Map[Any, Int]] = Map()
      tableDf.rdd.aggregate(zeroValueMap)(seqOp, immcombOp)
    }

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

}
