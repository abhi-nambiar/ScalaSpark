import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object WordCount2 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "wordcount2")

  val input = sc.textFile("C:/Users/Lenovo/Desktop/Documents/abhi/big data/week9/search_data.txt")
  
  val words = input.flatMap(_.split(" "))
  
  val wordsLower = words.map(_.toLowerCase())
      
  val wordmap = wordsLower.map((_,1))
  
  val finalCount = wordmap.reduceByKey(_+_)
  
  //val reversedTuple = finalCount.map(x => (x._2,x._1))
  
  val sortedValue = finalCount.sortBy(_._2, false)
  
  //val reversedResult = sortedValue.map(x => (x._2,x._1))
  
  sortedValue.collect.foreach(result)
  
  def result(a:(String,Int))= {
  val word = a._1
  val count = a._2
  println(s"$word: $count")
  }
  
}