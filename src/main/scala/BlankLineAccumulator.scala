import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object BlankLineAccumulator extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "BlankLineAccumulator")

  val input = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week10\\datasets/samplefile.txt")

  //accumulator can be of long or float
  val myAccum = sc.longAccumulator("blank line accumulator")

  //if line is blank, add 1 to accumulator
  input.foreach(x => if (x=="")myAccum.add(1))

  //find the total
  println(myAccum.value)

}