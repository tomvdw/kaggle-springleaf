package tom.kaggle.springleaf.app

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import tom.kaggle.springleaf.ApplicationContext
import tom.kaggle.springleaf.ml.{ FeatureVector, GbtReducedFeaturesEvaluator }
import tom.kaggle.springleaf.ml.SvmTrainer
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.sql.DataFrame

case class TrainModelApp(ac: ApplicationContext) {

  def run() {
    val scaledNumericalFeatures = "scaledNumericalFeatures"
    val pcaFeatures = "pcaFeatures"
    import ac.sqlContext.implicits._

    // Pre-process with ML library: http://spark.apache.org/docs/latest/ml-features.html
    val rawFeatureVectorsDF = ac.sc.objectFile[FeatureVector](ac.trainFeatureVectorPath, 16).toDF()
    val scaledData = scale(rawFeatureVectorsDF, "numericalFeatures", scaledNumericalFeatures)
    val pcaDF = reduce(scaledData, scaledNumericalFeatures, pcaFeatures, k = 50)

    val trainFeatureVectors = pcaDF
      .select("label", pcaFeatures).rdd
      .map(r => LabeledPoint(r.getAs[Double](0), r.getAs[Vector](1)))

    // Split data into sets for training, testing and validating
    val splits = trainFeatureVectors.randomSplit(Array(0.6, 0.2, 0.2))
    val (trainingSet, testSet, validationSet) = (splits(0), splits(1), splits(2))

    // Train model
    val numIterations = 100
    println(s"\nTraining model with $numIterations iterations")
    val model = SVMWithSGD.train(trainingSet, numIterations)
    model.clearThreshold()

    // Test model
    println(s"\nCompute raw scores on the test set.")
    val scoreAndLabels = testSet.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    println(s"\nArea under ROC = ${metrics.areaUnderROC()}")
    println(s"Evaluation: ${interpretAreaUnderROC(metrics.areaUnderROC())}")
  }

  private def scale(df: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler
    val scalerModel = scaler.fit(df)

    // Normalize each feature to have unit standard deviation.
    scalerModel.transform(df)
  }

  private def reduce(df: DataFrame, inputCol: String, outputCol: String, k: Int): DataFrame = {
    val pca = new PCA()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setK(k)
      .fit(df)
    pca.transform(df)
  }

  private def interpretAreaUnderROC(areaUnderROC: Double): String = {
    if (areaUnderROC < 0.6) "FAIL"
    else if (areaUnderROC < 0.7) "poor"
    else if (areaUnderROC < 0.8) "fair"
    else if (areaUnderROC < 0.9) "decent"
    else "excellent"
  }
}

object TrainModelApp {
  def main(args: Array[String]) {
    val configFilePath = if (args.length == 0) "application.conf" else args(0)
    val ac = new ApplicationContext(configFilePath)
    val app = TrainModelApp(ac)
    app.run()
  }

}
