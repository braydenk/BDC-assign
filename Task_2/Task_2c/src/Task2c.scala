import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext, x: String, y: String) {
    // Load each line of the input data
    val twitterLines = sc.textFile("hdfs://localhost/user/cloudera/twitter.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))

    println("Months: x = " + x + ", y = " + y)

    // Filter out any nul data or data with no tweets
    val filtered = twitterdata.filter(a => (a(1) != null && a(2) != null && a(3) != null && (a(2).toInt > 0 || a(2).toInt > 0)))

    // Seperate tweets from month X and Y
    val tweetsMonthX = filtered.filter(a => (a(1).equals(x))).map(a => (a(3), a(2).toInt))
    val tweetsMonthY = filtered.filter(a => (a(1).equals(y))).map(a => (a(3), a(2).toInt))

    // Join the to into (String (Int, Int))
    val joined = tweetsMonthX.join(tweetsMonthY)

    // NOTE: this may able to be simplified with a single reduce.
    val largestGrowth = joined.map({ a =>
                                val temp = a._2
                                val mX = temp._1
                                val mY = temp._2
                                (a._1, mX, mY, mY - mX)
                              }).reduce((acc, value) => {
                                if (acc._4 < value._4) value else acc
                              })

    // Print per requirements
    println("hashtagName: " + largestGrowth._1 + ", countX: " +
      largestGrowth._2 + ", countY: " + largestGrowth._3)
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Check command line arguments
    if(args.length != 2) {
      println("Expected two command line arguments: <month x> and <month y>")
    }
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task2c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext, args(0), args(1))
    // Stop Spark
    spark.stop()
  }
}
