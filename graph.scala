/** Databricks notebook source */
/** Analysing Graph of Interactions on MathOverflow using Spark on Databricks */

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

/** Define the data schema. */
val customSchema = StructType(Array(StructField("answerer", IntegerType, true),
  StructField("questioner", IntegerType, true), 
  StructField("timestamp", LongType, true)))

/** Load the data file. */
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "false")
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/mathoverflow.csv")
   .withColumn("date", from_unixtime($"timestamp"))
   .drop($"timestamp")
df.show()

/** Remove the edges with the same questioner and answerer.
 *  All the subsequent operations will be performed on this filtered data.
 */
val filteredDf = df.filter(!($"answerer" <=> $"questioner"))
filteredDf.show()

/** List the top 3 individuals who answered the most questions sorted in
  * descending order. If there is a ties, the one with lower node-id gets listed
  * first. That is, the 3 nodes with the highest out-degree.
  */
val top3Answerers = filteredDf.groupBy($"answerer")
  .agg(count($"answerer") as "questions_answered")
  .orderBy($"questions_answered".desc, $"answerer")
  .limit(3)
  .show

/** List the top 3 individuals who asked the most questions sorted in
  * descending order. If there is a tie, the one with lower node-id gets listed
  * first. That is, the 3 nodes with the highest in-degree.
  */
val top3Questioners = filteredDf.groupBy($"questioner")
  .agg(count($"questioner") as "questions_asked")
  .orderBy($"questions_asked".desc, $"questioner")
  .limit(3)
  .show

/** List The top 5 questioner-answerer pairs that are most common sorted in
  * descending order. If there is a tie, the answer and then the questioner
  * with lower value node-id gets listed first.
  */
val top5Pairs = filteredDf.groupBy($"answerer", $"questioner")
  .count()
  .orderBy($"count".desc, $"answerer", $"questioner")
  .limit(5)
  .show

/** List the number of interactions (questions asked & answered) over the
  * months from September 1, 2010 to December 31, 2010 sorted by months.
  * Reference: https://www.obstkel.com/blog/spark-sql-date-functions
  */

val interaction = filteredDf.filter(
  year($"Date") === lit(2010) && month($"Date").geq(lit(9)))
  .withColumn("Month", month($"Date"))
  .groupBy($"Month")
  .agg(count($"Month") as "total_interactions")
  .orderBy($"Month")
  .show

/** List the top 3 individuals with the maximum number of activities, i.e.
  * total questions asked or answered.
  */
val answerActivity = filteredDf.select($"answerer")
  .groupBy("answerer")
  .agg(count($"answerer") as "answer_activity")
val questionActivity = filteredDf.select($"questioner")
  .groupBy("questioner")
  .agg(count($"questioner") as "question_activity")

val naFillMap = Map("answer_activity" -> 0, "question_activity" -> 0)

val totalActivity = answerActivity.join(questionActivity,
  $"answerer" === $"questioner", "outer")
  /** Fill the null values out of outer joins. */
  .withColumn("answerer",
    when($"answerer".isNull, $"questioner").otherwise($"answerer"))
  .withColumn("questioner",
    when($"questioner".isNull, $"answerer").otherwise($"questioner"))
  .na.fill(na_fill_map)
  .withColumn("userID", $"answerer")
  .withColumn("total_activity", $"answer_activity" + $"question_activity")
  /** Alternative way of doing the above:
    * .select($"answer_activity", $"question_activity", 
    *   ($"answer_activity" + $"question_activity").as("total_activity"))
    */
  .select($"userID", $"total_activity")
  .orderBy($"total_activity".desc, $"userID")
  .limit(3)
  .show
