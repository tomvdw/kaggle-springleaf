package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

case class SvmTrainer(trainingDataset: RDD[LabeledPoint], components: Int, numIterations: Int) {

  lazy val model = {
    println(s"Training model on ${reducedFeatures.count()} instances")
    SVMWithSGD.train(reducedFeatures, numIterations)
      .clearThreshold()
  }

  def reduce(dataSet: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    dataSet.map(p => p.copy(features = pca.transform(p.features)))
  }

  private lazy val pca = {
    println("Fitting PCA model")
    new PCA(components).fit(trainingDataset.map(_.features))
  }

  private lazy val reducedFeatures: RDD[LabeledPoint] = {
    println("Reducing dataset")
    reduce(trainingDataset)
  }
}
