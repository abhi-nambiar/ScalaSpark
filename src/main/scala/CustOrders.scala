import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object CustOrders extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","custorders")
  
  val input = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/customerorders.csv")
  
  val newTuple = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))  //to sum, convert string to float
  
  val combineValue = newTuple.reduceByKey(_+_)
  
  val sortedTotal = combineValue.sortBy(_._2, false)
    
  val result = sortedTotal.collect  //this is not an rdd, all results collected into local machine
  
  result.foreach(println)
  
  
  
}