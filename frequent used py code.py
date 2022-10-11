# PANDAS DROP COLUMN
df = df.drop('column_name', axis=1)

# PANDAS RENAME COL
df2 = df.rename({'a': 'X', 'b': 'Y'}, axis=1)  # new method
df2 = df.rename({'a': 'X', 'b': 'Y'}, axis='columns')
#--assign the result back, as the modification is not-inplace. 
#--Alternatively, specify inplace=True:
df.rename({'a': 'X', 'b': 'Y'}, axis=1, inplace=True)

# REMOVE ROWS WHERE COLUMN CONTAINS CERTAIN CHARACTERS
dfa = df_all[(~df_all['Value'].str.contains("x"))]
dfa

# REMOVE ROWS WHERE THE "VALUE" COLUMN CONTAINS "X" OR ","" STRING, AND ASSIGN TO NEW DATAFRAME "DFA_2"
dfa_2 = df[(~df['Value'].str.contains("x|,", na=False))]

# COPY DATAFRAME TO NEW ONE BRINGING IN ONLY CERTAIN COLUMNS
new = old[['A', 'C', 'D']].copy()

# JOIN VALUES IN LIST 
list =  ['Ryan', 'is', 'the best']
j = '|'.join(list)
j
# 'Ryan|is|the best'

# SPLIT COLUMN AND ADD NEW COLUMNS TO DF
df[['Street', 'City', 'State']] = df['Address'].str.split(',', expand=True)
# display the dataframe
df

# CUSTOM NUMBER OF SPLITS
df['Address'].str.split(',', n=1, expand=True)

# CONCATENATE STRING COLUMNS COLUMNS INTO A SINGLE COLUMN. CONCAT COLUMNS BY SEPARATOR
df['Street'].str.cat(df[['City', 'State']], sep=',')


# BUILD A SEARCH STRING

dict_copy = df.to_dict('records')
search_str = []
for r in dict_copy:
    search_str.append(r['column_name'])




# READING DATA IN USING PYSPARK
STG_1_data = spark.read.csv("s3://tfsdl-aigbi-test/S1_09_17_2022data_all.csv", inferSchema=True, header=True)
