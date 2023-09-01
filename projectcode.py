spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_hospital = spark.read.option("header","true").csv("s3://buck1234/Capst/hospital.csv")
df_hospital.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_Patientsrecord = spark.read.option("header","true").csv("s3://buck1234/Capst/Patient_records.csv")
df_Patientsrecord.show()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZJ5SE5IUMHMNXYJM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_disease = spark.read.option("header","true").csv("s3://buck1234/Capst/disease.csv") 
df_disease.show()