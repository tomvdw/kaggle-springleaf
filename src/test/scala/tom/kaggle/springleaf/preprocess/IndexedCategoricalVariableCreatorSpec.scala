package tom.kaggle.springleaf.preprocess

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import tom.kaggle.springleaf.Names

class IndexedCategoricalVariableCreatorSpec extends FlatSpec with Matchers {
  val model1 = new StringIndexerModel("uuid", Array("label1", "label2"))
  val model2 = new StringIndexerModel("uuid", Array("label1", "label2", "label3"))
  val model3 = new StringIndexerModel("uuid", Array("label1"))

  private val variable1: String = "var1"
  private val variable2: String = "var2"
  private val variable3: String = "var3"

  private val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("test")

  "Transforming a DataFrame" should "result in all variables being indexed" in {
    val sc = new SparkContext(sparkConf)
    try {
      val sqlContext = new SQLContext(sc)
      val column1: String = Names.PrefixOfString + "1"
      val column2: String = Names.PrefixOfString + "2"
      val df = sqlContext.createDataFrame(Seq(
        ("a", "e"),
        ("b", "f"),
        ("b", "g"),
        ("a", "h")
      )).toDF(column1, column2)

      val creator = IndexedCategoricalVariableCreator(df)
      val transformedDf: DataFrame = creator.transformedDf

      transformedDf.count() should be(4)
      val distinctColumn1: Array[Row] = transformedDf.select(s"${Names.PrefixOfIndexedString}_$column2").distinct().collect()
      distinctColumn1.length should be(4)
      distinctColumn1.map(x => x.getDouble(0)).foreach(x => x >= 0 && x < 2)

      val distinctColumn2: Array[Row] = transformedDf.select(s"${Names.PrefixOfIndexedString}_$column2").distinct().collect()
      distinctColumn2.length should be(4)
      distinctColumn2.map(x => x.getDouble(0)).foreach(x => x >= 0 && x < 4)
    } finally {
      sc.stop()
    }
  }

  "Writing and reading label info of a transformed DataFrame" should "result in the correct numbers" in {
    val sc = new SparkContext(sparkConf)
    try {
      val sqlContext = new SQLContext(sc)
      val column1: String = Names.PrefixOfString + "1"
      val column2: String = Names.PrefixOfString + "2"
      val df = sqlContext.createDataFrame(Seq(
        ("a", "e"),
        ("b", "f"),
        ("c", "g"),
        ("d", "h")
      )).toDF(column1, column2)

      val creator = IndexedCategoricalVariableCreator(df)

      val tmpFile: String = System.getProperty("java.io.tmpdir") + "/tmp.json"
      IndexedCategoricalVariableCreator.saveArray(creator.models, tmpFile)
      val readArray = IndexedCategoricalVariableCreator.readArray(tmpFile)
      readArray.length should be(creator.models.size)
      readArray(0) should be(4)
      readArray(1) should be(4)
    } finally {
      sc.stop()
    }
  }

  "A map of a single StringIndexerModel" should "be stored as array to json" in {
    val modelMap: Map[String, StringIndexerModel] = Map(variable1 -> model1)
    val tmpFile: String = System.getProperty("java.io.tmpdir") + "/tmp.json"
    IndexedCategoricalVariableCreator.saveArray(modelMap, tmpFile)
    val readArray = IndexedCategoricalVariableCreator.readArray(tmpFile)
    readArray.length should be(modelMap.size)
    readArray(0) should be(2)
  }

  "A map of a multiple StringIndexerModel" should "be stored as array to json" in {
    val modelMap: Map[String, StringIndexerModel] =
      Map(variable1 -> model1,
        variable2 -> model2,
        variable3 -> model3)

    val tmpFile: String = System.getProperty("java.io.tmpdir") + "/tmp.json"
    IndexedCategoricalVariableCreator.saveArray(modelMap, tmpFile)
    val readArray: Array[Int] = IndexedCategoricalVariableCreator.readArray(tmpFile)

    readArray.length should be(modelMap.size)
    readArray(0) should be(model1.labels.length)
    readArray(1) should be(model2.labels.length)
    readArray(2) should be(model3.labels.length)
  }

  "A map of a single StringIndexerModel" should "be stored as map to json" in {
    val modelMap: Map[String, StringIndexerModel] =
      Map(variable1 -> model1,
        variable2 -> model2,
        variable3 -> model3)

    val tmpFile: String = System.getProperty("java.io.tmpdir") + "/tmp.json"
    IndexedCategoricalVariableCreator.saveMap(modelMap, tmpFile)
    val readArray = IndexedCategoricalVariableCreator.readMap(tmpFile)

    readArray.size should be(modelMap.size)
    modelMap.foreach { case (variable, model) =>
      readArray.get(variable).get should be(model.labels.length)
    }
  }

}
