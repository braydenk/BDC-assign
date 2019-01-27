import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    // TODO: Change back to hdfs
    val bankdataLines = sc.textFile("hdfs://localhost/user/cloudera/bank.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // Solution
    val soln = bankdata.sortBy(x => x(5).toInt, false)
                       .sortBy(x => x(3), true)
                       .map(x => (x(3), x(5), x(1), x(2), x(7)))

    soln.saveAsTextFile("Task_1d-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1d")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
