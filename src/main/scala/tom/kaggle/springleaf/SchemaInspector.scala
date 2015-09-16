package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField

case class SchemaInspector(df: DataFrame) {
  def getAllVariables = df.schema.fields.filter { x => x.name.startsWith("VAR") }
  def getCategoricalVariables = getAllVariables.filter { x => x.dataType == StringType }
  def getNumericalVariables = getAllVariables.filter { x => x.dataType != StringType }

  def getProcessedNumericalVariables(schema: StructType): Seq[StructField] =
    schema.filter { x => x.name.startsWith("DEC_") }
  def getProcessedNumericalVariables: Seq[StructField] =
    getProcessedNumericalVariables(df.schema)

  def getCategoricalColumns: List[Column] = getCategoricalVariables.map { x => df.col(x.name) }.toList
  def getNumericalColumns: List[Column] = getNumericalVariables.map { x => df.col(x.name) }.toList

  def showAllFields = df.schema.fields.foreach { x =>
    println("Name = %s, data type = %s, nullable = %s".format(x.name, x.dataType, x.nullable))
  }

}