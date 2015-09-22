package tom.kaggle.springleaf

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

case class SchemaInspector(df: DataFrame) {
  def getAllVariables = df.schema.fields.filter { x => x.name.startsWith("VAR") }

  def getCategoricalVariables = getAllVariables.filter { x => x.dataType == StringType }

  def getNumericalVariables = getAllVariables.filter { x => x.dataType != StringType }

  def getProcessedNumericalVariables(schema: StructType): Seq[StructField] = schema.filter { x => x.name.startsWith(ApplicationContext.prefixOfDecimal) }

  def getProcessedNumericalVariables: Seq[StructField] = getProcessedNumericalVariables(df.schema)

  def getProcessedCategoricalVariables(schema: StructType): Seq[StructField] = schema.filter { x => x.name.startsWith(ApplicationContext.prefixOfIndexedString) }

  def getProcessedCategoricalVariables: Seq[StructField] = getProcessedCategoricalVariables(df.schema)

  def getCategoricalColumns: List[Column] = getCategoricalVariables.map { x => df.col(x.name) }.toList

  def getNumericalColumns: List[Column] = getNumericalVariables.map { x => df.col(x.name) }.toList

  def showAllFields() {
    df.schema.fields.foreach { x =>
      println(s"Name = ${x.name}, data type = ${x.dataType}, nullable = ${x.nullable}")
    }
  }
}