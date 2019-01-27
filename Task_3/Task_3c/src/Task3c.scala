import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, queryWords: Seq[String]) {
    import spark.implicits._

    val wordsDF = spark.read.parquet("../docword_index.parquet").cache()

    println("Query words:")
    for(queryWord <- queryWords) {
      println(queryWord)
    }

    // Solution
    for(queryWord <- queryWords) {
      var sol = wordsDF.select("word", "docId")
        .where($"word" === queryWord)
        .orderBy(desc("count"))
        .take(1)

      println(sol(0))
    }
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3c")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
