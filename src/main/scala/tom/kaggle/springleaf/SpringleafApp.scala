package tom.kaggle.springleaf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType

object SpringleafApp {

  def main(args: Array[String]) {
    val ac = new ApplicationContext

    def getAllVariables(df: DataFrame) = df.schema.fields.filter { x => x.name.startsWith("VAR") }
    def getCategoricalVariables(df: DataFrame) = getAllVariables(df).filter { x => x.dataType == StringType }
    def getNumericalVariables(df: DataFrame) = getAllVariables(df).filter { x => x.dataType != StringType }

    def getCategoricalColumns(df: DataFrame): List[Column] = getCategoricalVariables(df).map { x => df.col(x.name) }.toList
    def getNumericalColumns(df: DataFrame): List[Column] = getNumericalVariables(df).map { x => df.col(x.name) }.toList

    def showAllFields(df: DataFrame) = df.schema.fields.foreach { x =>
      println("Name = %s, data type = %s, nullable = %s".format(x.name, x.dataType, x.nullable))
    }

    val startTime = System.currentTimeMillis()
    val df = ac.dataImporter.readCsv
    df.registerTempTable("xxx")
    val endReadTime = System.currentTimeMillis()

    println(df.first())

    //val groupedData = df.groupBy("VAR_0001").count().collect()
    //df.select("VAR_0001").distinct.foreach { x => println(x) }

    //    val variables = getAllVariables(df)
    //    val distinctValuesPerVariable = variables.map { v =>
    //      (v, df.groupBy(v.name).count().collect())
    //    }

    //val subDf = df.select(getNumericalColumns(df):_*)

    val numericalVariables = getNumericalVariables(df)
    val labelIndex = df.schema.fieldIndex("target")

    val lps = df.map { row =>
      {
        val doubles = for (column <- df.schema.fields) yield {
          val index = row.fieldIndex(column.name)
          if (!row.isNullAt(index)) {
            if (column.dataType == IntegerType) row.getAs[Integer](index).toDouble
            else if (column.dataType == LongType) row.getLong(index).toDouble
            else if (column.dataType == DoubleType) row.getDouble(index)
          }
          0.0 // TODO: impute missing values before!
        }
        val label = row.getInt(labelIndex).toDouble
        LabeledPoint(label, Vectors.dense(doubles))
      }
    }

    //    lps.take(1).foreach { x => println(x) }

    val pca = new PCA(10).fit(lps.map(_.features))
    val projected = lps.map(p => p.copy(features = pca.transform(p.features)))

    projected.take(1).foreach { x => println(x) }

    //    getAllVariables(df).foreach { x => println(x) }
    //    getCategoricalVariables(df).foreach { x => println(x) }
    //    val resultDf = ac.sqlContext.sql("SELECT COUNT(*) FROM xxx WHERE VAR_0001 IS NULL")
    //    resultDf.show()

    /**
     * Steps:
     *
     * For each numeric feature:
     * - impute missing values
     * For all numeric features:
     * - run PCA
     *
     * For all categorical features:
     * - do something with dates, e.g.:
     *   - detect what columns contain dates
     *   - parse dates
     *   - create features for year, month, day, week day, week in year, hour of day, etc
     * - encode booleans as 0 and 1?
     */

    val endQueryTime = System.currentTimeMillis()

    println("Reading: %f".format((endReadTime - startTime) / 1000.0))
    println("Query: %f".format((endQueryTime - endReadTime) / 1000.0))
    //groupedData.foreach { x => println(x) }

    //    val nrOfValuesPerVariable = distinctValuesPerVariable.map {
    //      case (variable, distinctValues) => (variable.name, distinctValues.size)
    //    }
    //
    //    val sortedValuesPerVariables = nrOfValuesPerVariable.sortBy {
    //      case (name, count) => count
    //    }
    //    sortedValuesPerVariables.foreach {
    //      case (name, count) => println("%s had %d distinct values".format(name, count))
    //    }
  }
}