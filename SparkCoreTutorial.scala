import org.apache.spark.sql.SparkSession

object SparkCoreTutorial extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("example")
    .getOrCreate()

  val sc = spark.sparkContext
  //Loading the text file
  val csvDF1 = sc.textFile("input.txt")

  //Splitting the sentences
  val counts = csvDF1.flatMap(line => line.split("-"))

  println("Number of rows in the document = " + csvDF1.count())
  println("Total number of words in file = " + counts.collect.length)

}
