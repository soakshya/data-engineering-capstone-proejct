spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df = spark.read.option("header","true").json("s3://buck1234/Capst/claims.json") 
df.show()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_disease = spark.read.option("header","true").csv("s3://buck1234/Capst/disease.csv") 
df_disease.show()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_group = spark.read.option("header","true").csv("s3://buck1234/Capst/group.csv") 
df_group.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subgroup = spark.read.option("header","true").csv("s3://buck1234/Capst/subgroup.csv") 
df_subgroup.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_hospital = spark.read.option("header","true").csv("s3://buck1234/Capst/hospital.csv") 
df_hospital.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subscriber = spark.read.option("header","true").csv("s3://buck1234/Capst/subscriber.csv") 
df_subscriber.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_grpsubgrp = spark.read.option("header","true").csv("s3://buck1234/Capst/grpsubgrp.csv") 
df_grpsubgrp.show()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_patients = spark.read.option("header","true").csv("s3://buck1234/Capst/Patient_records.csv") 
df_patients.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Find columns with null values
null_sums = []

for col_name in df_patients.columns:
    null_sum = df_patients.filter(col(col_name).isNull()).count()
    null_sums.append((col_name, null_sum))

print("Sum of null values in each column:")
for col_name, null_sum in null_sums:
    print(f'Column "{col_name}": {null_sum}')


rom pyspark.sql import SparkSession
from pyspark.sql.functions import col


# Find columns with null values
null_sums = []

for col_name in df_subscriber.columns:
    null_sum = df_subscriber.filter(col(col_name).isNull()).count()
    null_sums.append((col_name, null_sum))

print("Sum of null values in each column:")
for col_name, null_sum in null_sums:
    print(f'Column "{col_name}": {null_sum}')


df_subscriber = df_subscriber.na.fill("NA", subset=["first_name"])
df_subscriber = df_subscriber.na.fill("NA", subset=["Phone"])
df_subscriber = df_subscriber.na.fill("NA", subset=["Subgrp_id"])
df_subscriber = df_subscriber.na.fill("NA", subset=["Elig_ind"])


df_patient = df_patients.na.fill("NA", subset=["Patient_name"])
df_patient = df_patients.na.fill("NA", subset=["patient_phone"])


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subgroup.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.subgroup").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subgroup.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.subgroup").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()

df.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.claims").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_disease = df_disease.withColumnRenamed(" Disease_ID", "Disease_ID")


df_disease.write.csv("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.disease").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_group.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.group").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_grpsubgrp.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.grpsubgrp").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_hospital.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.hospital").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_patients.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.Patient_records").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()


df_subscriber=df_subscriber.withColumnRenamed("sub _id", "sub_id")
df_subscriber=df_subscriber.withColumnRenamed("Zip Code", "ZipCode")

df_subscriber.write.format("redshift").option("url", "jdbc:redshift://default-workgroup.639786871336.us-east-1.redshift-serverless.amazonaws.com:5439/dev").\
option("dbtable", "test.subscriber").\
option("aws_iam_role", "arn:aws:iam::639786871336:role/redshiftadmin").\
option("driver", "com.amazon.redshift.jdbc42.Driver").\
option("tempdir", "s3a://buck1234/output_path/1").\
option("user", "redshift").\
option("password", "Memory!1127").mode("overwrite").save()





