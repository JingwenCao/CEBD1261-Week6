package org.apache.spark.examples.sql
import org.apache.spark.sql.SparkSession
case class Insurance(key: Int, value: String)
object Week6ex2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // Load File
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load("insurance.csv")
    df.createOrReplaceTempView("insurance")
    // Get Size
    val size = df.count()
    println("Size of DataFrame")
    println(size)
    // Get Sex and Size of Sex
    val sexsize = spark.sql("SELECT COUNT(sex), sex FROM insurance GROUP BY sex").collect()
    println("Sex and count of each")
    sexsize.foreach(println)
    // Get smoker
    val smokersexsize = spark.sql("SELECT COUNT(sex), sex FROM insurance WHERE smoker='yes' GROUP BY sex").collect()
    println("Nonsmokers sex and count of each")
    smokersexsize.foreach(println)
    val chargesbyregion = spark.sql("SELECT region, SUM(charges) AS SumCharges FROM insurance GROUP BY region ORDER BY SumCharges DESC").collect()
    println("Charges by region descending")
    chargesbyregion.foreach(println)
  }
}
