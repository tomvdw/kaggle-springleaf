package tom.kaggle.springleaf.app

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import scaldi.{Injectable, TypesafeConfigInjector}
import tom.kaggle.springleaf.ml.{CategoricalFeatureInfoCreator, FeatureVector}
import tom.kaggle.springleaf.{SparkModule, SpringLeafModule}

object TrainModelApp extends App with Injectable {
  implicit val injector = TypesafeConfigInjector() :: new SparkModule :: new SpringLeafModule

  private val sc = inject[SparkContext]
  private val sqlContext = inject[SQLContext]
  private val categoricalFeatureInfoCreator = inject[CategoricalFeatureInfoCreator]
  private val dataPath = inject[String]("data.path.base")
  private val trainFeatureVectorPath = inject[String]("data.path.trainFeatureVector")

  val numClasses: Int = 2
  val numericalFeaturesName: String = "numericalFeatures"
  val categoricalFeaturesName: String = "categoricalFeatures"

  import sqlContext.implicits._

  run()

  def run() {
    val (trainFeatureVectors: RDD[LabeledPoint], categoricalInfo: Map[Int, Int]) =
      createFeatureVectorsForTraining(numberOfComponents = 0)

    // Split data into sets for training, testing and validating
    val splits = trainFeatureVectors.randomSplit(Array(0.6, 0.2, 0.2))
    val (trainingSet, testSet, validationSet) = (splits(0), splits(1), splits(2))

    //    lazy val svmModel = trainSVMWithSGD(trainingSet)

//    lazy val randomForest = trainRandomForest(trainingSet, categoricalInfo)
//    testModel(randomForest, testSet)

    lazy val gbt = trainGBT(trainingSet, categoricalInfo)
    testModel(gbt, testSet)
  }

  private def createFeatureVectorsForTraining(numberOfComponents: Int = 5): (RDD[LabeledPoint], Map[Int, Int]) = {
    val scaledNumericalFeatures = "scaledNumericalFeatures"
    val pcaFeatures = "pcaFeatures"

    println("Loading raw feature vector data from disk")
    val rawFeatureVectorsRDD: RDD[FeatureVector] = sc.objectFile[FeatureVector](trainFeatureVectorPath, 16)
    val rawFeatureVectorsDF: DataFrame = rawFeatureVectorsRDD.toDF()
    val firstFeatureVector = rawFeatureVectorsRDD.first()
    val numberOfNumericalFeatures = firstFeatureVector.numericalFeatures.size
    val numberOfCategoricalFeatures = firstFeatureVector.categoricalFeatures.size

    val (pcaDF, featuresColumn, numOfFeatures: Int) = if (numberOfComponents > 0) {
      println("Scaling numerical data")
      val scaledData = scale(rawFeatureVectorsDF, numericalFeaturesName, scaledNumericalFeatures)

      println(s"Performing PCA with $numberOfComponents components")
      val reducedDataFrame: DataFrame = reduce(scaledData, scaledNumericalFeatures, pcaFeatures, numberOfComponents)
      (reducedDataFrame, pcaFeatures, numberOfComponents)
    } else (rawFeatureVectorsDF, numericalFeaturesName, numberOfNumericalFeatures)

    println("Creating categorical features info")
    val categoricalInfo: Map[Int, Int] = categoricalFeatureInfoCreator.createFrom(
      numberOfComponents = numOfFeatures,
      numberOfCategoricalFeatures = numberOfCategoricalFeatures)

    println("Constructing final feature vectors")
    val trainFeatureVectors: RDD[LabeledPoint] = pcaDF
      .select("label", featuresColumn, categoricalFeaturesName)
      .map(r => {
        val numericFeatures: Array[Double] = r.getAs[Vector](1).toArray
        val categoricalFeatures: Array[Double] = r.getAs[Vector](2).toArray
        if ((numOfFeatures + numberOfCategoricalFeatures) != (numericFeatures.length + categoricalFeatures.length)) {
          throw new RuntimeException(s"Numeric: $numberOfNumericalFeatures / ${numericFeatures.length} and Categorical: $numberOfCategoricalFeatures / ${categoricalFeatures.length}")
        }
        val features: Vector = Vectors.dense(numericFeatures ++ categoricalFeatures)
        LabeledPoint(r.getAs[Double](0), features)
      })

    (trainFeatureVectors, categoricalInfo)
  }

  private def trainGBT(trainingSet: RDD[LabeledPoint],
                       categoricalFeaturesInfo: Map[Int, Int],
                       numIterations: Int = 100,
                       maxDepth: Int = 8): Vector => Double = {
    val maxNumberOfCategories = categoricalFeaturesInfo.map(_._2).max

    val boostingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = numIterations
    boostingStrategy.treeStrategy.numClasses = numClasses
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = categoricalFeaturesInfo
    boostingStrategy.treeStrategy.maxBins = maxNumberOfCategories

    val model = GradientBoostedTrees.train(trainingSet, boostingStrategy)
    model.save(sc, s"$dataPath/gbt.model")
    model.predict
  }

  private def trainRandomForest(trainingSet: RDD[LabeledPoint],
                                categoricalFeaturesInfo: Map[Int, Int],
                                numTrees: Int = 250,
                                featureSubsetStrategy: String = "auto",
                                impurity: String = "gini",
                                maxDepth: Int = 8,
                                maxBins: Int = 32,
                                numIterations: Int = 500): Vector => Double = {

    val maxNumberOfCategories = categoricalFeaturesInfo.map(_._2).max
    println(s"Max number of categories is $maxNumberOfCategories")
    val model = RandomForest.trainClassifier(trainingSet, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, scala.math.max(maxBins, maxNumberOfCategories))
    model.predict
  }

  private def trainSVMWithSGD(trainingSet: RDD[LabeledPoint], numIterations: Int = 500): Vector => Double = {
    println(s"\nTraining model with $numIterations iterations")
    trainingSet.cache()
    val model = SVMWithSGD.train(trainingSet, numIterations)
    model.clearThreshold()
    model.predict
  }

  private def testModel(predict: Vector => Double, testSet: RDD[LabeledPoint]): Unit = {
    println(s"\nCompute raw scores on the test set.")
    val scoreAndLabels = testSet.map { point =>
      val score = predict(point.features)
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

