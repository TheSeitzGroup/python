#reading data in using pyspark
STG_1_data = spark.read.csv("s3://tfsdl-aigbi-test/S1_09_17_2022data_all.csv", inferSchema=True, header=True)


#remove rows where column contains certain characters
dfa = df_all[(~df_all['Value'].str.contains("x"))]
dfa
