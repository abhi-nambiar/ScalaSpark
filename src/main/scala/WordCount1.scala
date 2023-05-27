import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordcount")

  val input = sc.textFile("C:/Users/Lenovo/Desktop/data/file1.txt")

  val words = input.flatMap(x => x.split(" "))

  val wordmapping = words.map(x => (x,1))

  val wordfinal = wordmapping.reduceByKey((x,y) => x+y)

  wordfinal.collect.foreach(println)

}