import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.collect_list
object SparkAssignment1_Question_b {


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
      .csv("src/main/resources/user.csv")

    userDf.printSchema()
    userDf.show(false)
    val transactionDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/transaction.csv")

    transactionDf.show()

    val joinedDf = userDf.join(transactionDf, userDf("user_id") === transactionDf("userid")) //inner join by default
    joinedDf.show()


    val df2 = joinedDf.groupBy("userid").agg(collect_list("product_description")
      .as("uniqueProductBought"))
    println("products bought by each user")

    df2.show(true)

  }
}