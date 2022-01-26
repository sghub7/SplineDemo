from pyspark.sql import SparkSession

#  SparkSession
spark = SparkSession.builder.appName("SplineDemo").getOrCreate()

applicants = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("data/input/applicants_table.csv")
)
applicants.createOrReplaceTempView("applicants")


fraudScores = (
    spark.read.option("header", "true")
    .option("inferschema", "true")
    .csv("data/input/fraud_scores.csv")
)
fraudScores.createOrReplaceTempView("fraudScores")

# Join fraudScores and applicant tabes to see if there is a match and create applicant fraud score table 
applicant_fraud_score = spark.sql("""
SELECT
  applicants.id as id,
  fraudScores.fraudscore as fraudScore,
  applicants.applied_limit as appliedLimit,
  case when fraudScores.fraudscore > 50 then "Reject" else "Approve" end as creditDecision
  
FROM applicants
JOIN fraudScores
ON applicants.id = fraudScores.id
""")

applicant_fraud_score.cache()

# Write output and print 
applicant_fraud_score.write.mode("overwrite").csv("data/output/applicant_fraud_score")
applicant_fraud_score.show(20, False)

# Aggregate and derive  the total amount of approved and rejected loans
applicant_fraud_score.createOrReplaceTempView("applicant_fraud_score")
credit_summary = spark.sql("""
SELECT
  sum(appliedLimit) as Amount,
  creditDecision as status
  
FROM applicant_fraud_score
group by creditDecision

""")

# Write output and print 
credit_summary.write.mode("overwrite").csv("data/output/credit_summary")
credit_summary.show(20, False)