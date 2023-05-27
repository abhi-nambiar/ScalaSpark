import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object BigDataCampaign extends App {

  //datatype used is set because we need to consider each boring word only once
  //this code is running on driver machine locally
  def loadBoringWords():Set[String] = {
    var boringWords:Set[String] = Set()  //var because we want to append values

    //boring words will not be an rdd as rdd will be split across many machines, so we dont use textFile to read it
    //we want it in local, create a set and then broadcast it to all machines
    val lines = Source.fromFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week10\\datasets/boringwords.txt").getLines()

    for(line <- lines){
      boringWords += line  //lines will be appended from the file
    }
  boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "BigDataCampaign")

  var nameSet = sc.broadcast(loadBoringWords)

  val input = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week10\\datasets/bigdatacampaign.csv")

  //split data based on comma, will return an array
  //we need the search term(1st col) and cost(11th col)
  val rdd2 = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  //eg: (30.24, big data course)

  //now we need to split based on words, every word on new line and amount next to it
  //we are calc the amount paid for each word
  val rdd3 = rdd2.flatMapValues(x => x.split(" "))
  //eg: (30.24, big), (30.24, data), (30.24, course)

  //swap key and value
  val rdd4 = rdd3.map(x => (x._2.toLowerCase(),x._1))  //will treat big and BIG as same
  //eg: (big,30.24), (data,30.24), (course,30.24)

  //to check if it is a boring word or not
  //remove if it is in the boring words list
  val filterValues = rdd4.filter(x => !nameSet.value(x._1))  //.value gives boolean values

  //aggregate value (cost) to find total money spent on each word
  //money will be added for the same word searched
  val rdd5 = filterValues.reduceByKey((x,y) => x+y)

  //sorted based on highest money spent on ads
  val rdd6 = rdd5.sortBy(x => x._2, false)

  //to find top 20 words
  rdd6.take(20).foreach(println)

  //scala.io.StdIn.readLine()

}