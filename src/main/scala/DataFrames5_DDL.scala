import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object DataFrames5_DDL extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","new application4")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersSchemaDDL = "orderid Int, orderdate String, custid Int, orderstatus String"

  //dataframe reader api
  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .schema(ordersSchemaDDL)
    .option("path","C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/orders.csv")
    .load

  ordersDf.printSchema

  ordersDf.show()

  //scala.io.StdIn.readLine()
  spark.stop()

}