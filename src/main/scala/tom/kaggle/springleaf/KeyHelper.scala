package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructField

case class KeyHelper(ac: ApplicationContext) {
  val separator = ":"
  val columnPrefix = keyify("column", ac.fraction)

  def keyFor(column: StructField): String = keyFor(column.name)

  def keyFor(columnName: String): String = keyify(columnPrefix, columnName)

  def keyPattern: String = keyify(columnPrefix, "*")

  def nameOf(key: String): String = key.split(separator).last

  private def keyify(parts: Any*): String = parts.mkString(separator)
}