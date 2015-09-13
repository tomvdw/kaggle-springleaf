package tom.kaggle.springleaf

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame

case class SchemaInspector(ac: ApplicationContext) {
  def getAllVariables = ac.df.schema.fields.filter { x => x.name.startsWith("VAR") }
  def getCategoricalVariables = getAllVariables.filter { x => x.dataType == StringType }
  def getNumericalVariables = getAllVariables.filter { x => x.dataType != StringType }

  def getCategoricalColumns: List[Column] = getCategoricalVariables.map { x => ac.df.col(x.name) }.toList
  def getNumericalColumns: List[Column] = getNumericalVariables.map { x => ac.df.col(x.name) }.toList

  def showAllFields = ac.df.schema.fields.foreach { x =>
    println("Name = %s, data type = %s, nullable = %s".format(x.name, x.dataType, x.nullable))
  }

}