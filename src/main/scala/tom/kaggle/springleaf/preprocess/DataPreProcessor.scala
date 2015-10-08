package tom.kaggle.springleaf.preprocess

import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, IntegerType, StructField}
import org.apache.spark.sql.{DataFrame, Row}
import tom.kaggle.springleaf.SchemaInspector

case class DataPreProcessor(df: DataFrame, categoricalTransformer: CategoricToIndexTransformer) {
  val schemaInspector = SchemaInspector(df)
  val labelIndex = df.schema.fieldIndex("target")

  def getLabel(row: Row): Double = row.getInt(labelIndex).toDouble

  def transformCategoricalToIndexed = categoricalTransformer.transform

  def extractNumericalValue(row: Row, column: StructField): Option[Double] = {
    val index = row.fieldIndex(column.name)
    if (!row.isNullAt(index)) {
      if (column.dataType == IntegerType) Some(row.getAs[Integer](index).toDouble)
      else if (column.dataType == LongType) Some(row.getLong(index).toDouble)
      else if (column.dataType == DoubleType) Some(row.getDouble(index))
    }
    None
  }

  def getNumericalValues(row: Row): Vector = {
    /*
    val y = schemaInspector.getNumericalVariables.zipWithIndex
      .map { case (c, i) => (i, extractNumericalValue(row, c)) }
      .map { case (i, Some(v)) => (i, v) }

    val z = schemaInspector.getNumericalVariables.zipWithIndex.map {
      case (c, i) => {
        extractNumericalValue(row, c) match {
          case Some(value) => (i, value)
        }
      }
    }
    */

    val sparseValues = for {
      (column, index) <- schemaInspector.getNumericalVariables.zipWithIndex
      extractedValue <- extractNumericalValue(row, column)
      value <- Some(extractedValue)
    } yield (index, value)
    Vectors.sparse(row.size, sparseValues)

    /*
    val doubles =
      for ((column, index) <- schemaInspector.getNumericalVariables.zipWithIndex)
        yield extractNumericalValue(row, column)
    Vectors.dense(doubles)
     */
  }

  def getNumericalFeatures: RDD[LabeledPoint] = {
    df.map { row => LabeledPoint(getLabel(row), getNumericalValues(row)) }
  }

  def getImputedNumericalFeatures: RDD[LabeledPoint] = {
    val featuresWithMissingValues = getNumericalFeatures
    featuresWithMissingValues.map { lp => {
      //      lp.features.
    }
    }
    ???
  }

  def principalComponentAnalysis(components: Int, features: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val pca = new PCA(components).fit(features.map(_.features))
    features.map(p => p.copy(features = pca.transform(p.features)))
  }

}
