import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object AvgConnections extends App {
  
  def parseLine(line: String) = {
    val fields = line.split("::")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age,numFriends)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","avgconnections")
  
  val input = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/friendsdata.csv")
  
  val mappedInput = input.map(parseLine)
  
  val mappedFinal = mappedInput.mapValues(x => (x,1))
  
  val totalsByAge = mappedFinal.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val averagesByAge = totalsByAge.map(x => (x._1, x._2._1/x._2._2)).sortBy(x => x._1)
  
  averagesByAge.collect.foreach(println)
}