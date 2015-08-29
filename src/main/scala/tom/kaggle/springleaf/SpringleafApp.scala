package tom.kaggle.springleaf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SpringleafApp extends App {
  val conf = new SparkConf().setAppName("Kaggle Springleaf").setMaster("local[*]").set("spark.executor.memory", "4g")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf)

  val dataFolderPath = "/Users/tomvanderweide/kaggle/springleaf/"
  val trainFilePath = dataFolderPath + "train.csv"

  val trainData = sc.textFile(trainFilePath, 16)
  
  val parsedTrain = trainData.map(line => {
    val parts = line.split(",")
    parts
  })
  
  parsedTrain.take(2).foreach(x => println(x))
  
  
}