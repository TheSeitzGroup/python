# PANDAS DROP COLUMN
df = df.drop('column_name', axis=1)

# PANDAS RENAME COL
df2 = df.rename({'a': 'X', 'b': 'Y'}, axis=1)  # new method
df2 = df.rename({'a': 'X', 'b': 'Y'}, axis='columns')
#--assign the result back, as the modification is not-inplace. 
#--Alternatively, specify inplace=True:
df.rename({'a': 'X', 'b': 'Y'}, axis=1, inplace=True)

#reading data in using pyspark
STG_1_data = spark.read.csv("s3://tfsdl-aigbi-test/S1_09_17_2022data_all.csv", inferSchema=True, header=True)


#remove rows where column contains certain characters
dfa = df_all[(~df_all['Value'].str.contains("x"))]
dfa

# remove rows where the 'Value' column contains 'x' or ',' string, and assign to new dataframe "dfa_2"
dfa_2 = df[(~df['Value'].str.contains("x|,", na=False))]

# JOIN VALUES IN LIST 
list =  ['Ryan', 'is', 'the best']
j = '|'.join(list)
j
# 'Ryan|is|the best'

# split column and add new columns to df
df[['Street', 'City', 'State']] = df['Address'].str.split(',', expand=True)
# display the dataframe
df

# custom number of splits
df['Address'].str.split(',', n=1, expand=True)

# concatenate string columns columns into a single column
# concat columns by separator
df['Street'].str.cat(df[['City', 'State']], sep=',')
