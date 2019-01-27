import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("hdfs://localhost/user/cloudera/docword.txt").
      as[Docword]
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("hdfs://localhost/user/cloudera/vocab.txt").
      as[VocabWord]

    // Solution
    def firstLetter(x: String): String = {
      x(0).toString
    }

    val firstLetterUDF = spark.udf.register[String, String]("firstLetter", firstLetter)

    val solution = docwords
        .join(vocab, "vocabId")
        .orderBy($"vocabId")
        .select($"word", $"docId", $"count", firstLetterUDF($"word").as("firstLetter"))

    solution.show(10)
    solution.write.mode("overwrite").parquet("../docword_index.parquet")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
