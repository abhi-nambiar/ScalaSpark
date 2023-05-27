import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object AgeCheck extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "agecheck")

  val input = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week9/age_data.dataset1")

  val dataMap = input.map(line => {
    val fields = line.split(",")
    if (fields(1).toInt > 18)
      (fields(0),fields(1),fields(2),"Y")
    else
      (fields(0),fields(1),fields(2),"N")
  })

  dataMap.collect.foreach(println)

}