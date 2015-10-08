package tom.kaggle.springleaf.app

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf.ml.FeatureVector
import tom.kaggle.springleaf.{SparkModule, SpringLeafModule}

object TrainModelApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sc = inject[SparkContext]
  private val sqlContext = inject[SQLContext]
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")

  import sqlContext.implicits._

  run()

  def run() {
    val scaledNumericalFeatures = "scaledNumericalFeatures"
    val pcaFeatures = "pcaFeatures"

    // Pre-process with ML library: http://spark.apache.org/docs/latest/ml-features.html
    val rawFeatureVectorsDF = sc.objectFile[FeatureVector](trainFeatureVectorPath, 16).toDF()
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
    trainingSet.cache()
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

