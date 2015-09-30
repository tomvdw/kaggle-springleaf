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

  private def getFeatureVector(row: Row): FeatureVector = {
    try {
      val label = row.getAs[String](labelIndex).toDouble
      FeatureVector(label, getNumericalValues(row), getCategoricalValues(row))
    } catch {
      case e: Throwable => {
        println(s"Getting value of $labelIndex, but could not make a double out of ${row.get(labelIndex)}")
        throw e
      }
    }
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
    val sparseValues = for {
      (column, index) <- columns.zipWithIndex
      value <- row.get(index) match {
        case value: Number => Some(value.doubleValue())
        case _ => None
      }
    } yield (index, value)
    Vectors.sparse(row.size, sparseValues)
  }
}
