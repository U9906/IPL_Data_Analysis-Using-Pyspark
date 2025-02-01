# Databricks notebook source
from pyspark.sql import SparkSession
#create session
spark=SparkSession.builder.appName("IPL Data Analysis").getOrCreate()


# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.functions import col, when, sum, avg, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import lower, regexp_replace
from pyspark.sql.functions import current_date, expr

# COMMAND ----------

#reading the dataset
ball_by_ball_df = spark.read.format("csv").option("header","true").load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")

ball_by_ball_df.display()


# COMMAND ----------

#defining schema to spark---StructFeild, StructType, IntegerType,StringType,BooleanType,DateType,DecimalType
from pyspark.sql.types import StructField, StructType, IntegerType,StringType,BooleanType,DateType,DecimalType
ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])
ball_by_ball_df= spark.read \
              .format("csv") \
              .schema(ball_by_ball_schema) \
              .option("header","true") \
              .load("s3://ipl-data-analysis-project/Ball_By_Ball.csv")      
                               

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])
match_df = spark.read.schema(match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Match.csv")

     

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df = spark.read.schema(player_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player.csv")


# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

player_match_df = spark.read.schema(player_match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Player_match.csv")

     

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

team_df = spark.read.schema(team_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-project/Team.csv")
     

# COMMAND ----------

#filter to include only valid deliveries(excluding wides and no balls)
ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))
#ball_by_ball_df.display()

#Aggregation: Calculate the total and average runs scored in each match and inning
Total_and_avg_runs = ball_by_ball_df.groupby("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("Avg_runs")
)
Total_and_avg_runs.display()


# COMMAND ----------

#Window fudtions: Calculation running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df = ball_by_ball_df.withColumn(
    "running_total",sum("runs_scored").over(windowSpec)
)

# COMMAND ----------

#Conditional formataing: Flag high impacet balls either a wicket or more than 6 runs including extras

ball_by_ball_df=ball_by_ball_df.withColumn(
    "high_impact_flag",
    when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True),True).otherwise(False)
)

# COMMAND ----------

#ball_by_ball_df.show(5)
display(ball_by_ball_df)

# COMMAND ----------

# Extracting year, month, and day from the match date for more detailed time-based analysis
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("day",dayofmonth("match_date"))

#High margin win: categorization win amrgin into low, medium high
match_df = match_df.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)

#Analyse the impact of toss: who wins the toss and match
match_df = match_df.withColumn(
    "Toss_Match_winner",
    when((col("toss_winner")) == (col("match_winner")), "Yes").otherwise("No")
)

# COMMAND ----------

# Normalize and clean player names(Replaces any character that is not a letter, digit, or space with an empty string)
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# Categorizing players based on batting hand
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
)

# Show the modified player DataFrame
player_df.show(2)

# COMMAND ----------

# Add a 'veteran_status' column based on player age
player_match_df = player_match_df.withColumn("veteran_status",
                                             when(col("age_as_on_match")>= 35,"VETERAN").otherwise("NON_VETERAN"))

# Dynamic column to calculate years since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

#player_match_df.show(5)
#player_match_df.display()

# COMMAND ----------

### USING SPARK SQL 
ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

#%sql
top_scoring_batsmen_per_season = spark.sql("""
SELECT p.player_name, b.season,SUM(b.runs_scored) AS total_runs 
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id   
JOIN player p ON pm.player_id = p.player_id
WHERE b.striker = pm.player_id
GROUP BY p.player_name, b.season
ORDER BY b.season, total_runs DESC 
""")
top_scoring_batsmen_per_season.display()

# COMMAND ----------

top_match_winner_by_toss = spark.sql("""
                                   select match_id,toss_winner,match_winner,
                                   CASE when toss_winner = match_winner then "WON" ELSE "LOSS" END AS match_outcome from match WHERE toss_name IS NOT NULL order by match_id """)

top_match_winner_by_toss.show(2)
