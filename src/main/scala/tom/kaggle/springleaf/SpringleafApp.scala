package tom.kaggle.springleaf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object SpringleafApp {

  def main(args: Array[String]) {
    val ac = new ApplicationContext

    def getAllVariables(df: DataFrame) = df.schema.fields.filter { x => x.name.startsWith("VAR") }

    val startTime = System.currentTimeMillis()
    val df = ac.dataImporter.readCsv
    val endReadTime = System.currentTimeMillis()

    //val groupedData = df.groupBy("VAR_0001").count().collect()
    //df.select("VAR_0001").distinct.foreach { x => println(x) }

//    val variables = getAllVariables(df)
//    val distinctValuesPerVariable = variables.map { v =>
//      (v, df.groupBy(v.name).count().collect())
//    }

    df.registerTempTable("xxx")
    
    val resultDf = ac.sqlContext.sql("SELECT COUNT(*) FROM xxx WHERE VAR_0001 IS NULL")
    resultDf.show()

    /**
     * Steps:
     *
     * For each numeric feature:
     * - impute missing values
     * For all numeric features:
     * - run PCA
     *
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

    def showAllFields = df.schema.fields.foreach { x =>
      println("Name = %s, data type = %s, nullable = %s".format(x.name, x.dataType, x.nullable))
    }
  }
}