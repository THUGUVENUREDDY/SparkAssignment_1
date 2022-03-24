import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, collect_list}
object SparkAssignment1_Question_a {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    val userDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep",",")
      .csv("src/main/resources/user.csv")

    userDf.printSchema()
    //userDf.show(false)

    val transactionDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/transaction.csv")

    transactionDf.printSchema()

    transactionDf.show()

    val joinedDf = userDf.join(transactionDf, userDf("user_id") === transactionDf("userid")) //inner join by default
    joinedDf.show()

    joinedDf.printSchema()

    val df1 = joinedDf.groupBy("product_description").agg(collect_list(col("location"))
      .as("uniqueLoc"))
    println(" unique locations where each product is sold ")
    df1.show(false)
  }
}