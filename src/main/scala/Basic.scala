package dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object Basic {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val spark =
      SparkSession
        .builder()
        .master("local[2]")
        .appName("Dataset-Basic")
        .getOrCreate()
    import spark.implicits._
    // Create a tiny Dataset of integers
    val s = Seq(10, 11, 12, 13, 14, 15)
    val ds = s.toDS()
    ds.collect().foreach(e => println(e))
    ds.show()
    println("*** columns and schema")
    ds.printSchema()
    println("*** column types")
    ds.dtypes.foreach(println(_))
    println("*** schema as if it was a DataFrame")
    println("*** values > 12")
    ds.where($"value" > 12).show()
    // ordered and shown in a nice way
    ds.where($"value" > 12).foreach(println(_))
    //since its using different nodes its not ordered

    // This seems to be the best way to get a range that's actually a Seq and
    // thus easy to convert to a Dataset, rather than a Range, which isn't.
    val s2 = Seq.range(1, 100)
    println("*** size of the range")
    println(s2.size)
    val tuples = Seq((1, "one", "un"), (2, "two", "deux"), (3, "three", "trois"))
    val tupleDS = tuples.toDS()
    println("*** Tuple Dataset types")
    println(tupleDS.dtypes)
    tupleDS.printSchema()
    // the tuple columns have unfriendly names, but you can use them to query
    println("*** filter by one column and fetch another")
    tupleDS.show()
    tupleDS.where(col("_1").gt(2)).select(col("_2"), col("_3")).show()

    spark.stop();
  }

}
