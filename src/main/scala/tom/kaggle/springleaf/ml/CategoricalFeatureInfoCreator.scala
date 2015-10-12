package tom.kaggle.springleaf.ml

import tom.kaggle.springleaf.preprocess.IndexedCategoricalVariableCreator

case class CategoricalFeatureInfoCreator(numberOfLabelsPerVariablePath: String) {

  def createFrom(numberOfComponents: Int, numberOfCategoricalFeatures: Int): Map[Int, Int] = {
    val numberOfLabelsPerVariable: Array[Int] = IndexedCategoricalVariableCreator.readArray(numberOfLabelsPerVariablePath)
    (for (i <- 0 until numberOfCategoricalFeatures) yield i + numberOfComponents -> numberOfLabelsPerVariable(i)).toMap
  }
}
