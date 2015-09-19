package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

case class SvmTrainer(trainingDataSet: RDD[LabeledPoint], components: Int, numIterations: Int) {
  println("Fitting PCA model")
  val pca = new PCA(components).fit(trainingDataSet.map(_.features))

  def reduce(dataSet: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    dataSet.map(p => p.copy(features = pca.transform(p.features)))
  }

  println("Reducing dataset")
  val reducedFeatures = reduce(trainingDataSet)
  reducedFeatures.cache()

  val model = {
    println(s"Training model on ${reducedFeatures.count()} instances")
    val m = SVMWithSGD.train(reducedFeatures, numIterations)
    m.clearThreshold()
    m
  }

}