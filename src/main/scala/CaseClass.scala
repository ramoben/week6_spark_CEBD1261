package dataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object CaseClass {
  case class Number(i: Int, english: String , french: String )
  def main (args: Array[ String ]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =
      SparkSession. builder ()
        .appName( "Dataset-CaseClass" )
        .master( "local[4]" )
        .getOrCreate()
    import spark.implicits._
    val numbers = Seq (
      Number ( 1 , "one" , "un" ) ,
      Number ( 2 , "two" , "deux" ) ,
      Number ( 3 , "three" , "trois" ))
    val numberDS=numbers.toDS()
    println ( "Dataset Types" )
    numberDS.printSchema()
    println ( "filter dataset where i>1" )
    numberDS.filter(numberDS("i")>1).show()
    println ( "select the number with English column and display" )
    numberDS.drop("i","french").show()
    println ( "select the number with English column and filter for i>1" )
    numberDS.filter(numberDS("i")>1).drop("i","french").show()
    println ( "sparkSession dataset" )
    val anotherDS=spark.createDataset(numbers)
    println ( "Spark Dataset Types" )
    anotherDS.printSchema()
  }
}