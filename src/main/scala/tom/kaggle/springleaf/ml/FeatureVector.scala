package tom.kaggle.springleaf.ml

import org.apache.spark.mllib.linalg.Vector

case class FeatureVector(label: Double, numericalFeatures: Vector, categoricalFeatures: Vector)
