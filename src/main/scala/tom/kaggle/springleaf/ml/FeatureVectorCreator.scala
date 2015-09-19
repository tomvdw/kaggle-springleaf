package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import tom.kaggle.springleaf.{ApplicationContext, SchemaInspector}

case class FeatureVectorCreator(df: DataFrame) {
  private val schemaInspector = SchemaInspector(df)
  private val labelIndex = df.schema.fieldIndex(ApplicationContext.labelFieldName)

  def getFeatureVectors: RDD[LabeledPoint] = df.map(getFeatureVector)

  private def getFeatureVector(row: Row): LabeledPoint =
    LabeledPoint(row.getInt(labelIndex).toDouble, getNumericalValues(row))

  private def getNumericalValues(row: Row): Vector = {
    val numericColumns = schemaInspector.getProcessedNumericalVariables(row.schema)
    val sparseValues = for {
      (column, index) <- numericColumns.zipWithIndex
      value <- row.get(index) match {
        case value: Number => Some(value.doubleValue())
        case _ => None
      }
    } yield (index, value)
    Vectors.sparse(row.size, sparseValues)
  }
}
