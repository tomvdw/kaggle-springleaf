package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructField

case class KeyHelper(fraction: Double) {
  val separator = ":"
  val columnPrefix = keyify("column", fraction)

  def keyFor(column: StructField): String = keyFor(column.name)

  def keyFor(columnName: String): String = keyify(columnPrefix, columnName)

  def keyPattern: String = keyify(columnPrefix, "*")

  def nameOf(key: String): String = key.split(separator).last

  private def keyify(parts: Any*): String = parts.mkString(separator)
}
