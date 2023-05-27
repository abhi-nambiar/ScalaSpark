import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}

object DataFrames3 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  //val ordersDf = spark.read
   // .format("json")
   // .option("path","C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/players.json")
   // .option("mode","DROPMALFORMED")
   // .load

  val ordersDf = spark.read
    .format("parquet")
    .option("path","C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/users.parquet")
    .load

  ordersDf.printSchema

  ordersDf.show(false)

  //scala.io.StdIn.readLine()
  spark.stop()

}