package tom.kaggle.springleaf.app

import scaldi.Injectable

object SpringleafApp extends Injectable {

  //  def main(args: Array[String])(implicit injector: Injector) {
  //    val ac = new ApplicationContext()
  //
  //    val startTime = System.currentTimeMillis()
  //    val endReadTime = System.currentTimeMillis()
  //
  //    val castDf = ac.df.selectExpr("cast(VAR_0006 AS DECIMAL) as CVAR_0006")
  //    castDf.printSchema()
  //    castDf.registerTempTable("castxxx")
  //    ac.sqlContext.sql("SELECT count(1) as isnull from castxxx where CVAR_0006 is null").show(100)
  //    ac.sqlContext.sql("SELECT CVAR_0006, count(1) from castxxx group by CVAR_0006").show(100)
  //
  //    val nullDf = ac.sqlContext.sql("SELECT case when VAR_0006 = 'null' then NULL else VAR_0006 end as VAR_0006 from xxx")
  //    nullDf.registerTempTable("nullxxx")
  //    nullDf.printSchema()
  //
  //    ac.sqlContext.sql("SELECT count(1) as isnull from nullxxx where VAR_0006 is null").show(100)
  //    ac.sqlContext.sql("SELECT count(1) as equalsnull from nullxxx where VAR_0006 = 'null'").show(100)
  //    ac.sqlContext.sql("SELECT VAR_0006, count(1) from nullxxx group by VAR_0006").show(100)
  //    ac.sqlContext.sql("SELECT avg(VAR_0006) from nullxxx").show()
  //
  //    //    ac.sqlContext.sql("SELECT COUNT(1) FROM xxx").show()
  //    //    println("Number of rows: " + df.count())
  //
  //    /**
  //     * Steps:
  //     *
  //     * For each numeric feature:
  //     * - impute missing values
  //     *   OR use StandardScaler and use 0.0 as mean :)
  //     * For all numeric features:
  //     * - run PCA
  //     *
  //     * For all categorical features:
  //     * - do something with dates, e.g.:
  //     *   - detect what columns contain dates
  //     *   - parse dates
  //     *   - create features for year, month, day, week day, week in year, hour of day, etc
  //     * - encode booleans as 0 and 1?
  //     */
  //
  //    //val groupedData = df.groupBy("VAR_0001").count().collect()
  //    //df.select("VAR_0001").distinct.foreach { x => println(x) }
  //
  //    //    val variables = getAllVariables(df)
  //    //    val distinctValuesPerVariable = variables.map { v =>
  //    //      (v, df.groupBy(v.name).count().collect())
  //    //    }
  //
  //    //val subDf = df.select(getNumericalColumns(df):_*)
  //
  //    val dataPreProcessor = DataPreProcessor(ac)
  //
  //    val (indDf, indexedNames) = dataPreProcessor.transformCategoricalToIndexed
  //
  //
  //    indDf.printSchema()
  //    indDf.registerTempTable("yyy")
  //    ac.sqlContext.sql("SELECT ind_VAR_0001, COUNT(*) FROM yyy group by ind_VAR_0001").show()
  //
  //    //val catFeatures = df.map { row => LabeledPoint(row.getInt(labelIndex).toDouble, getNumericalValues(row)) }
  //
  //    //    lps.take(1).foreach { x => println(x) }
  //
  //    val projected = dataPreProcessor.principalComponentAnalysis(10, dataPreProcessor.getNumericalFeatures)
  //    projected.take(1).foreach { x => println(x) }
  //
  //    val endQueryTime = System.currentTimeMillis()
  //
  //    println("Reading: %f".format((endReadTime - startTime) / 1000.0))
  //    println("Query: %f".format((endQueryTime - endReadTime) / 1000.0))
  //    //groupedData.foreach { x => println(x) }
  //
  //    //    val nrOfValuesPerVariable = distinctValuesPerVariable.map {
  //    //      case (variable, distinctValues) => (variable.name, distinctValues.size)
  //    //    }
  //    //
  //    //    val sortedValuesPerVariables = nrOfValuesPerVariable.sortBy {
  //    //      case (name, count) => count
  //    //    }
  //    //    sortedValuesPerVariables.foreach {
  //    //      case (name, count) => println("%s had %d distinct values".format(name, count))
  //    //    }
  //  }
}
