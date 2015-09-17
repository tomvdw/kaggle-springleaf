package tom.kaggle.springleaf.app

import tom.kaggle.springleaf.ApplicationContext
import org.apache.spark.mllib.regression.LabeledPoint
import tom.kaggle.springleaf.ml.PrincipalComponentAnalysis
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.PCA

case class TrainModelApp(ac: ApplicationContext) {

  def run {
    val trainFeatureVectors = ac.sc.objectFile[LabeledPoint](ApplicationContext.trainFeatureVectorPath, 16)
    trainFeatureVectors.cache()
    println("Nr of training instances " + trainFeatureVectors.count())

    val components = 50

    val pca = new PCA(components).fit(trainFeatureVectors.map(_.features))
    val reducedFeatures = trainFeatureVectors.map(p => p.copy(features = pca.transform(p.features)))

    reducedFeatures.cache()
    println("Nr of reduced features instances " + reducedFeatures.count())
    reducedFeatures.take(10).foreach(println)

    val numIterations = 100
    val model = SVMWithSGD.train(reducedFeatures, numIterations)
    model.clearThreshold()

  }

}

object TrainModelApp {
  def main(args: Array[String]) {
    val ac = new ApplicationContext
    val app = TrainModelApp(ac)
    app.run
  }

}