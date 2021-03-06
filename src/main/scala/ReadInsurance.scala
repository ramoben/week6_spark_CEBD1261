
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object ReadInsurance {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName("WordCount")
      .getOrCreate()
    //1. Read insurance.csv file (uploaded in slack channel week6)
    val input = session.read.csv("insurance_csv.csv")
    input.printSchema()
    //2. Print the size in lines
    val insuranceDF = input.toDF()
    println(insuranceDF.count)
    //2. Print the size in bytes
    import org.apache.spark.util.SizeEstimator
    println(SizeEstimator.estimate(insuranceDF))
    //3. Print sex and count of sex (use group by in sql)
    insuranceDF.show()
    insuranceDF.groupBy("_c1").count().show(false)
    //4. Filter smoker=yes and print again the sex,count of sex
    insuranceDF.filter(insuranceDF("_c4")==="yes").groupBy("_c1").count().show(false)
    //5. Group by region and sum the charges (in each region), then print rows by
    //descending order (with respect to sum)
    val df2 = insuranceDF.withColumn("_c6", insuranceDF("_c6") cast "Int" as "charges")
    df2.show()
    df2.groupBy("_c5").sum("_c6").sort(col("sum(_c6)").desc).show(false)

  }
}