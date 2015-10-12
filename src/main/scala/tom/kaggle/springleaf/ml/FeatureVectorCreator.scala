package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row}
import tom.kaggle.springleaf.{Names, SchemaInspector}

case class FeatureVectorCreator(df: DataFrame) {
  private val schemaInspector = SchemaInspector(df)
  private val labelIndex = df.schema.fieldIndex(Names.LabelFieldName)

  def getFeatureVectors: RDD[FeatureVector] = df.map(getFeatureVector)

  private def getFeatureVector(row: Row): FeatureVector = {
    val label: Double = getLabel(row)
    val categoricalValues: Vector = getCategoricalValues(row)
    val numericalValues: Vector = getNumericalValues(row)
    FeatureVector(label, numericalValues, categoricalValues)
  }

  private def getNumericalValues(row: Row): Vector = {
    val numericColumns = schemaInspector.getProcessedNumericalVariables(row.schema)
    getVector(row, numericColumns)
  }

  private def getCategoricalValues(row: Row): Vector = {
    val categoricalColumns = schemaInspector.getProcessedCategoricalVariables(row.schema)
    getVector(row, categoricalColumns)
  }

  private def getVector(row: Row, columns: Seq[StructField]): Vector = {
    val sparseValues: Seq[(Int, Double)] =
      for {
        (column, index) <- columns.zipWithIndex
        value <- row.getAs[Any](column.name) match {
          case n: Number => Some(n.doubleValue())
          case _ => None
        }
      } yield (index, value)
    Vectors.sparse(columns.size, sparseValues)
  }

  private def getLabel(row: Row): Double = {
    try {
      row.getAs[String](labelIndex).toDouble
    } catch {
      case e: Throwable =>
        println(s"Getting value of $labelIndex, but could not make a double out of ${row.get(labelIndex)}")
        throw e
    }
  }

}
