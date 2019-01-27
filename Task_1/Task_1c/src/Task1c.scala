import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    // TODO: Change this back to hdfs!
    val bankdataLines = sc.textFile("hdfs://localhost/user/cloudera/bank.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // Solution
    val soln = bankdata.collect({
      case a if (a(5).toInt < 501) => ("Low", 1)
      case b if (b(5).toInt > 500 && b(5).toInt < 1501) => ("Medium", 1)
      case c if (c(5).toInt > 1501) => ("High", 1)
    }).reduceByKey(_ + _)

    // Output to textfile
    soln.saveAsTextFile("Task_1c-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
