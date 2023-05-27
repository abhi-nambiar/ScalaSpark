import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class OrdersData (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

object DataFrames2 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf: Dataset[Row] = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/orders.csv")

  import spark.implicits._
  //this import is req when we convert from a df to ds
  // _ is like *

  val ordersDs = ordersDf.as[OrdersData]

  ordersDs.filter(x => x.order_id < 10)

  //ordersDf.filter("order_ids < 10").show

  //scala.io.StdIn.readLine()
  spark.stop()

}