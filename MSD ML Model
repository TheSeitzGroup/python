# Databricks notebook source
import pandas as pd
import numpy as np
import mlflow

# COMMAND ----------

#function to list all files 
def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths


# COMMAND ----------

#paths = get_dir_content('s3://tfsdl-aigbi-test/single_input/')
#[print(p) for p in paths]

# COMMAND ----------

#job parameters
try:
  input_file = dbutils.widgets.get("input_file")
except:
  print("not able to pull input_file from job parameters, using default value")
  input_file = 'input.csv'
finally:
  file_name = 's3://tfsdl-aigbi-test/single_input/'+input_file

# BRING IN S3 data -- single input file (.alg file converted to .csv)
input_data = spark.read.csv(file_name, inferSchema=True, header=True)

# COMMAND ----------

# READ MODELS DICTIONARY MAPPING FILE from S3
dict_file_name = 's3://tfsdl-aigbi-test/dictionary/models_dictionary_V2.csv'
dict_data = spark.read.csv(dict_file_name, inferSchema=True, header=True)
df = dict_data.toPandas() 

#create models dictionary
models_dictionary = {}

for index,row in df.iterrows():
  models_dictionary[row['model_name']] = {}
  models_dictionary[row['model_name']]['V1'] = {}
  models_dictionary[row['model_name']]['V1']['inputs'] = row['inputs'].split(",")
  models_dictionary[row['model_name']]['V1']['number_inputs'] = len(row['inputs'].split(","))
  models_dictionary[row['model_name']]['V1']['outputs'] = row['outputs'].split(",")
  models_dictionary[row['model_name']]['V1']['number_outputs'] = len(row['outputs'].split(","))

df.head()


# COMMAND ----------

# READ file with upper / lower limits for each output feature
file_name = 's3://tfsdl-aigbi-test/outputs_spec_limits_table_V2.csv'
limits_data = spark.read.csv(file_name, inferSchema=True, header=True)

limits_df = limits_data.toPandas() 

# COMMAND ----------

limits_df.head()

# COMMAND ----------

df_all = input_data.toPandas()

# COMMAND ----------

df_all.head()
#print(df_all.shape)

# COMMAND ----------

#df_all.info()
if df_all['Value'].dtype == 'float64':
  print('float')
else:
  print('string')

# COMMAND ----------

#cleaning + transformations
# remove strings from 'Value' col
if df_all['Value'].dtype != 'float64':
  dfa = df_all[(~df_all['Value'].str.contains("x|,", na=False))]
else:
  dfa = df_all.copy()
print(dfa.columns)

if ('\\' in df_all['FileName'][0]):
  dfa[['FN_1', 'FN_2']] = dfa['FileName'].str.rsplit('\\', n=1, expand=True)
  dfa2 = dfa.drop(['FN_1', 'LastWriteTime','FileName'], axis=1)
  dfa2.rename({'FN_2': 'FileName'}, axis=1, inplace=True)

else:
  dfa2 = dfa.drop(['LastWriteTime'], axis=1)
  #dfa2.rename({'FileName': 'Machine'}, axis=1, inplace=True)

dfa2['FeatureName'] = dfa2['Header_1'].map(str) +'.'+ dfa2['Header_2'].map(str) +'.'+ dfa2['Header_3'].map(str) +'.'+ dfa2['Header_4'].map(str) +'.'+ dfa2['Header_5'].map(str) #.agg('.'.join, axis=1)
d3 = dfa2
print(d3.shape)
d3['Value'] = d3['Value'].astype(float)
#make copy of d3 to d4
d4 = d3.copy()


# COMMAND ----------

d4

# COMMAND ----------

#categorize features into input/output groups

d4['in_out_num'] = np.where(np.logical_and(np.logical_or(d4['Header_2'].str.startswith('SA'),d4['Header_2'].str.startswith('Mh')),d4['Header_3'].isin(['MagnCorr'])),'input2', 

				   np.where(np.logical_and(np.logical_or(np.logical_or(d4['Header_2'].str.startswith('HM'),d4['Header_2'].str.startswith('LM')),d4['Header_2'].str.startswith('Ml')),d4['Header_3'].isin(['MagnCorr'])),'output2',

				   np.where(d4['Header_1'].isin(['Beam LM', 'Beam NanoProbe', 'Eftem LM', 'Eftem NanoProbe', 'Gun', 'Image LM', 'Image NanoProbe','Stem NanoProbe', 'MdlMonochromatorMgr', 'Monochromator']),'input1',

                   np.where(d4['Header_1'].isin(['Beam MicroProbe', 'Eftem MicroProbe','Stem LM', 'Image MicroProbe', 'Stem MicroProbe', 'Column']),'output1',
                   
                   np.where(d4['Header_3'].isin(['ObjStigm', 'ComaStigm', 'ImageCorA1CoarseStigm', 'StigToStig', 'IBS_OSt_Factor', 'Dist', 'Obj3Stigm', 'Obj3Factor', 'ObjFactor', 'PP_Obj', 'PP_Diffr', 'DiffrFactor']),'input3',
                   
                   np.where(np.logical_and(d4['Header_3'].isin(['DiffrStigm']),d4['Header_2'].str.contains('HM x D x nolor')),'input3',
                   
                   np.where(d4['Header_3'].isin(['DiffrStigm']),'output3',
                   
                   np.where(np.logical_and(d4['Header_3'].isin(['DistCorr']),d4['Header_2'].isin(['SA x I x zoom'])),'input3',
                   
                   np.where(d4['Header_3'].isin(['DistCorr']),'output3',
                   
                   np.where(d4['Header_3'].isin(['GunFactor', 'PP_Gun', 'GunStigm', 'CondFactor', 'CondStigm', 'PP_Cond', 'A1CoarseStigm', 'BS_CondFactor', 'ProbeCorA1CoarseStigm']),'input4',
                   
                   np.where(d4['Header_3'].isin(['Cond3Stigm', 'Cond3Factor', 'Cond3to2Stigm']),'output4',
                   
                   'none')))))))))))

# COMMAND ----------

#remove features based on header_5 ... these features are not required based on William's input
indexMono = d4[(d4['in_out_num']=='input1') & (d4['FeatureName'].isin(['Monochromator.GunlensOffsetMatrixForAccelerating.PotRange003.None.Excitation','Monochromator.GunlensOffsetMatrixForDecelerating.PotRange004.None.Excitation','Monochromator.GunlensOffsetMatrixForDecelerating.PotRange005.None.Excitation','Monochromator.GunlensOffsetMatrixForDecelerating.PotRange008.None.Excitation','Monochromator.OffsetMatrixForAccelerating.PotRange003.None.dE1y/Pot','Monochromator.OffsetMatrixForAccelerating.PotRange003.None.dE2x/Pot','Monochromator.OffsetMatrixForAccelerating.PotRange003.None.dE2y/Pot','Monochromator.OffsetMatrixForAccelerating.PotRange003.None.Excitation','Monochromator.OffsetMatrixForDecelerating.PotRange004.None.dE1y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange004.None.dE2x/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange004.None.dE2y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange004.None.Excitation','Monochromator.OffsetMatrixForDecelerating.PotRange005.None.dE1y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange005.None.dE2x/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange005.None.dE2y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange005.None.Excitation','Monochromator.OffsetMatrixForDecelerating.PotRange008.None.dE1y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange008.None.dE2x/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange008.None.dE2y/Pot','Monochromator.OffsetMatrixForDecelerating.PotRange008.None.Excitation']))].index
d4.drop(indexMono, inplace=True)

# COMMAND ----------

d4['FeatureName_io'] = d4['FeatureName'] +"_"+ d4['in_out_num']
d4.head()

# COMMAND ----------

#remove duplicates
d4_unique=d4.drop_duplicates(subset=['FileName','FeatureName_io'], keep = 'first')
pivot_data = d4_unique.pivot(index='FileName', columns='FeatureName_io', values='Value')

# COMMAND ----------

pivot_data.head()

# COMMAND ----------

#PREDICTIONS LOOP

model_prefix = 'AIG_MSD_'
version = 'V1'
output_dict = {}

for model in models_dictionary:
  test_model = model
  model_name = model_prefix + test_model

  #model_version_uri = "models:/{model_name}/1".format(model_name=model_name)
  model_version_uri = "models:/{model_name}/1".format(model_name=model_name)

  print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_version_uri))
  model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

  #model_production_uri = "models:/{model_name}/production".format(model_name=model_name)
  #print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_production_uri))
  #model_production = mlflow.pyfunc.load_model(model_production_uri)

  col_list = models_dictionary[test_model][version]['inputs']
  X_new = pivot_data.reindex(columns=col_list)

  #prediction
  pred = model_version_1.predict(X_new)

  #convert prediction array to dataframe and add column names
  pred_df = pd.DataFrame(pred, columns = models_dictionary[test_model][version]['outputs'])

  #convert df to dictionary
  pred_dict = pred_df.to_dict('list')

  #append predicitons to output object
  output_dict.update(pred_dict)



# COMMAND ----------

#convert output dictionary to dataframe
output_df = pd.DataFrame(output_dict)

# COMMAND ----------

output_df.shape

# COMMAND ----------

## LOGIC TO REPLACE PREDICTION WITH LABEL 'NOT FOUND' IF OUTPUT PARAMETER WAS NOT PRESENT IN INPUT.CSV FILE ## 
orig_file_cols = pivot_data.columns

for col in output_df.columns:
  if col in orig_file_cols:
    print('feature found: ' + col)
  else:
    print('feature NOT found: ' + col)
    output_df[col] = 'Not found' 

# COMMAND ----------

final_output = output_df.melt(var_name="FeatureName_io", value_name="Prediction")
final_output['lower_limit']=''
final_output['upper_limit']=''
final_output.head()


# COMMAND ----------

for i in range(len(final_output)):
  final_output.at[i,'lower_limit'] = limits_df['lower_limit'][limits_df['FeatureName_io'] == final_output.loc[i, "FeatureName_io"]].values[0]
  final_output.at[i,'upper_limit'] = limits_df['upper_limit'][limits_df['FeatureName_io'] == final_output.loc[i, "FeatureName_io"]].values[0]

final_output.head()

# COMMAND ----------

machine = pivot_data.index.values[0]

machine = machine.replace('.alg','')

if 'csv' in machine:
  pass
else:
  machine = machine + '.csv'

final_output.to_csv('/dbfs/FileStore/tables/temp/prediction_'+machine, index=False)

# COMMAND ----------

dbutils.fs.mv('/FileStore/tables/temp/prediction_'+machine, 's3://tfsdl-aigbi-test/single_output/prediction_'+machine)
