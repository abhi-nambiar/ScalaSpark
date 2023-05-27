import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

object DataFrame6_write extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","new application6")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  //dataframe reader api
  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/orders.csv")
    .load

  val ordersRep = ordersDf.repartition(4)

  //dataframe writer api
  ordersDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path","C:\\Users\\Lenovo\\Desktop/newfolder1")
    .save()

  //scala.io.StdIn.readLine()
  spark.stop()

}