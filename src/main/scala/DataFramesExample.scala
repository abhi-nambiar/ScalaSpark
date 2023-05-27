import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFramesExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")

  /*val spark = SparkSession.builder()  //to build a spark session
    .appName("My application 1")  //this name will show up in UI when we build our job
    .master("local[2]")   //running it on local with 2 cores
    .getOrCreate()  //only one instance of spark session available*/

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/orders.csv")

  //ordersDf.show()
  //this shows 20 rows by default

  //ordersDf.printSchema()

  val groupOrders = ordersDf
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()
  //all these are transformations not action

  //groupOrders.foreach(x => {
   // println(x)
 // })

  groupOrders.show(50)

  Logger.getLogger(getClass.getName).info("My application has completed successfully")
  //scala.io.StdIn.readLine()
  spark.stop()

}