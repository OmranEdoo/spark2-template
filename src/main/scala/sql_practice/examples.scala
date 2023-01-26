package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)
  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demographieDF = spark.read.json("data/input/demographie_par_commune.json")
    val departementDF = spark.read.csv("data/input/departements.txt")

    // Total number of people in France
    demographieDF.select($"Population").agg(sum($"Population").as("Population")).show()

    // Population par département
    val pop_dep = demographieDF.select($"Population", $"Departement").groupBy($"Departement").sum("Population").sort(desc("sum(Population)"))
    pop_dep.show(10)

    // Population par département avec nom des départements
    departementDF.select($"_c0".as("name"), $"_c1".as("code"))
    pop_dep
      .join(departementDF, pop_dep("Departement") === departementDF("_c1"))
      .sort(desc("sum(Population)"))
      .show(10)
  }


  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample_07DF = spark.read
      .option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_07")

    val sample_08DF = spark.read
      .option("header", false)
      .option("sep", "\t")
      .csv("data/input/sample_08")

    // 1. Find top salaries in 2007 which are above $100k
    sample_07DF
      .select($"_c0".as("code"), $"_c1".as("job"), $"_c3".as("salary"))
      .where($"salary" > 100000)
      .sort($"salary".desc)
      .show(10)

    // 2. Growth between 2008 and 2007
    sample_07DF
      .join(sample_08DF, sample_07DF("_c0") === sample_08DF("_c0"), "Inner")
      .select(sample_07DF("_c1"), sample_07DF("_c3"), sample_08DF("_c3"), sample_08DF("_c3") - sample_07DF("_c3") as "growth")
      .where(sample_07DF("_c3") < sample_08DF("_c3") and sample_07DF("_c3") > 100000 and sample_08DF("_c3") > 100000)
      .sort(desc("growth"))
      .show(10)

    // 3. Job loss
    sample_07DF
      .join(sample_08DF, sample_07DF("_c0") === sample_08DF("_c0"), "Inner")
      .select(sample_07DF("_c1"), sample_07DF("_c3"), sample_08DF("_c3"), sample_08DF("_c3") - sample_07DF("_c3") as "job_loss")
      .where(sample_07DF("_c3") < sample_08DF("_c3"))
      .sort(desc("job_loss"))
      .show(10)
  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    //toursDF.show(20)

    toursDF
      .select("tourDifficulty")
      .groupBy("tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show()

    toursDF
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show()

    toursDF
      .groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show()

    toursDF
      .groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"), min("tourLength"), max("tourLength"), avg("tourLength"))
      .show()

    val topTags = toursDF.select($"tourName", explode($"tourTags") as "tags")
      .groupBy("tags")
      .count()
      .orderBy($"count".desc)
    topTags.show(10)

    val TagsDifficulty = toursDF.select(explode($"tourTags") as "tags", $"tourDifficulty")
      .groupBy("tags", "tourDifficulty")
      .count()
      .orderBy($"count".desc)
    TagsDifficulty.show(10)

    val TagsDifficultyavg = toursDF.select(explode($"tourTags") as "tags", $"tourDifficulty", $"tourPrice")
      .groupBy("tags", "tourDifficulty")
      .agg(min("tourPrice") as ("mintourPrice"),
        max("tourPrice") as ("maxtourPrice"),
        avg("tourPrice") as ("avgtourPrice"))
      .orderBy($"avgtourPrice".desc)
    TagsDifficultyavg.show(10)
  }
}
