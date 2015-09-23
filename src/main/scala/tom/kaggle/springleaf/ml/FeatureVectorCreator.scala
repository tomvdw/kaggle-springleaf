package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField

import tom.kaggle.springleaf.Names
import tom.kaggle.springleaf.SchemaInspector

case class FeatureVectorCreator(df: DataFrame) {
  private val schemaInspector = SchemaInspector(df)
  private val labelIndex = df.schema.fieldIndex(Names.LabelFieldName)

  def getFeatureVectors: RDD[FeatureVector] = df.map(getFeatureVector)

  private def getFeatureVector(row: Row): FeatureVector =
    FeatureVector(row.getInt(labelIndex).toDouble, getNumericalValues(row), getCategoricalValues(row))

  private def getNumericalValues(row: Row): Vector = {
    val numericColumns = schemaInspector.getProcessedNumericalVariables(row.schema)
    getVector(row, numericColumns)
  }

  private def getCategoricalValues(row: Row): Vector = {
    val categoricalColumns = schemaInspector.getProcessedCategoricalVariables(row.schema)
    getVector(row, categoricalColumns)
  }

  private def getVector(row: Row, columns: Seq[StructField]): Vector = {
    val sparseValues = for {
      (column, index) <- columns.zipWithIndex
      value <- row.get(index) match {
        case value: Number => Some(value.doubleValue())
        case _             => None
      }
    } yield (index, value)
    Vectors.sparse(row.size, sparseValues)
  }
}
