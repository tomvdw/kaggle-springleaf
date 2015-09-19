package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint

case class GbtReducedFeaturesEvaluator(trainingDataSet: RDD[LabeledPoint], components: Int, numIterations: Int) {
  println("Fitting PCA model")
  val pca = new PCA(components).fit(trainingDataSet.map(_.features))

  def reduce(dataSet: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    dataSet.map(p => p.copy(features = pca.transform(p.features)))
  }

  val boostingStrategy = BoostingStrategy.defaultParams("Classification")
  boostingStrategy.numIterations = numIterations
  boostingStrategy.treeStrategy.numClasses = 2
  boostingStrategy.treeStrategy.maxDepth = 5
  //  Empty categoricalFeaturesInfo indicates all features are continuous.
  boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

  val model = GradientBoostedTrees.train(reduce(trainingDataSet), boostingStrategy)
}