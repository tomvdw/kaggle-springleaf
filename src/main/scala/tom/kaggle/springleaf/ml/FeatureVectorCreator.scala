package tom.kaggle.springleaf.ml

import java.lang.Double

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row}
import tom.kaggle.springleaf.{ApplicationContext, SchemaInspector}

case class FeatureVectorCreator(df: DataFrame) {
  private val schemaInspector = SchemaInspector(df)
  private val labelIndex = df.schema.fieldIndex(ApplicationContext.labelFieldName)

  def getFeatureVector: RDD[LabeledPoint] = {
    df.map { row => LabeledPoint(getLabel(row), getNumericalValues(row)) }
  }

  private def getLabel(row: Row): Double = row.getInt(labelIndex).toDouble

  private def getNumericalValues(row: Row): Vector = {
    val numericalVariables = schemaInspector.getProcessedNumericalVariables(row.schema)
    val sparseValues: Seq[(Int, scala.Double)] = for {
      (column, indexInFeatureVector) <- numericalVariables.zipWithIndex
      value <- extractNumericalValue(row, column)
    } yield (indexInFeatureVector, value)
    Vectors.sparse(row.size, sparseValues)
  }

  private def extractNumericalValue(row: Row, column: StructField): Option[scala.Double] = {
    val indexInRow = row.fieldIndex(column.name)
    if (row.isNullAt(indexInRow)) None
    else row.get(indexInRow) match {
      case value: Number => Some(value.doubleValue())
      case _ => None
    }
  }

}