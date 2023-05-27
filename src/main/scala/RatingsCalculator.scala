import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object RatingsCalculator extends App {
 
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","custorders")
  
  val input = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/moviedata.data")
  
  val mappedIp = input.map(x => x.split("\t")(2))
  
  //val mappedValue = mappedIp.map((_,1))
  //instead of using map and then reduce by key, we can directly count by value
  
  //val combinedValue = mappedValue.reduceByKey(_+_)
    
  //val sortedRes = combinedValue.sortByKey(false)
  
  //sortedRes.collect.foreach(println)
  
  val combinedValue = mappedIp.countByValue.foreach(println)
  
  //difficult to sort because it is an rdd after action
  
  //only use countByValue if it is the last step because it is not an rdd
  //otherwise it will hamper parallelism
  //any operation after this step will happen on local machine
  
}