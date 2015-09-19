package tom.kaggle.springleaf.ml

import java.lang.Double
import java.math.BigDecimal

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField

import tom.kaggle.springleaf.ApplicationContext
import tom.kaggle.springleaf.SchemaInspector

case class FeatureVectorCreater(df: DataFrame) {
  private val schemaInspector = SchemaInspector(df)
  private val labelIndex = df.schema.fieldIndex(ApplicationContext.labelFieldName)

  def getFeatureVector: RDD[LabeledPoint] = {
    df.map { row => LabeledPoint(getLabel(row), getNumericalValues(row)) }
  }

  private def getLabel(row: Row): Double = row.getInt(labelIndex).toDouble

  private def getNumericalValues(row: Row): Vector = {
    val numericalVariables = schemaInspector.getProcessedNumericalVariables(row.schema)
    val sparseValues: Seq[(Int, scala.Double)] = for {
      (column, index) <- numericalVariables.zipWithIndex
      optionalValue <- extractNumericalValue(row, column)
      value <- Some(optionalValue)
    } yield (index, value)
    Vectors.sparse(row.size, sparseValues)
  }

  private def extractNumericalValue(row: Row, column: StructField): Option[scala.Double] = {
    val index = row.fieldIndex(column.name)
    if (row.isNullAt(index)) return None
    else parseDouble(row.get(index))
  }

  private def parseDouble(rawValue: Any): Option[scala.Double] = {
    rawValue match {
      case value: Integer => Some(value.toDouble)
      case value: BigDecimal =>
        if (value == null) None
        else Some(value.doubleValue())
      case value: Long => Some(value.toDouble)
      case value: Double => Some(value)
      case default => None
    }
  }

}