# Databricks notebook source
# MAGIC %sh
# MAGIC # below is trying to install lib 'ODBC Driver 18 for SQL Server'
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get -q -y install msodbcsql18
# MAGIC pip3 install pyodbc
# MAGIC pip3 install --force-reinstall -v "sqlalchemy==1.4.46"
# MAGIC pip3 install pandas

# COMMAND ----------

from distutils.log import error
import email
import json
import urllib
import boto3
import pandas
import io
import os
import pyodbc
from datetime import datetime
import sqlalchemy
from sqlalchemy.sql.expression import false, true
from sqlalchemy.orm import sessionmaker
from pathlib import Path

# COMMAND ----------

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env").lower().strip()
dbutils.widgets.text("object_name", "sales target")
object_name = dbutils.widgets.get("object_name").lower().strip()
dbutils.widgets.text("key_name", "")
key_name = dbutils.widgets.get("key_name").replace('+', ' ')
dbutils.widgets.text("archive_path", "")
archive_path = dbutils.widgets.get("archive_path").replace('+', ' ')
dbutils.widgets.text("archive_datetime", "")
archive_datetime = dbutils.widgets.get("archive_datetime")
dbutils.widgets.text("bucket_name", "")
bucket_name = dbutils.widgets.get("bucket_name").replace('+', ' ')

aws_access_key = dbutils.secrets.get("cad_bi_scope", "aws_access_key")
aws_access_secret = dbutils.secrets.get("cad_bi_scope", "aws_access_secret")
cad_user_id = dbutils.secrets.get("cad_bi_scope", "cad_user_id")
cad_user_pwd = dbutils.secrets.get("cad_bi_scope", "cad_user_pwd")

sql_server_dev = 'awsu1-07wcwd01d'
sql_server_qa = 'awsu1-07wcwd01q'
sql_server_prod = 'awsu1-10wcwd04p'

# COMMAND ----------

"""### LOCAL_TESTING

with open('./script/localsettings.json') as f:    
    local_settings = json.load(f)

cad_user_id = local_settings["sql_user"]
cad_user_pwd = local_settings["sql_password"]
aws_access_key = local_settings["aws_access_key"]
aws_access_secret = local_settings["aws_access_secret"]
env = local_settings["env"].lower().strip()
object_name = local_settings["object_name"]
bucket_name = local_settings["aws_s3_bucket"]
key_name = local_settings["key_name"]
archive_path = local_settings["archive_path"]
archive_datetime = local_settings["archive_datetime"]

sql_server_dev = local_settings["sql_server_dev"]
sql_server_qa = local_settings["sql_server_qa"]
sql_server_prod = local_settings["sql_server_prod"]

"""### LOCAL_TESTING

staging_db_dev = 'Dev_Staging'.lower()
staging_db_qa = 'QA_Staging'.lower()
staging_db_prod = 'CDW_Staging'.lower()
reference_db_dev = 'Dev_Reference'.lower()
reference_db_qa = 'QA_Reference'.lower()
reference_db_prod = 'CDW_Reference'.lower()
harmonized_db_dev = 'Dev_Harmonized'.lower()
harmonized_db_qa = 'QA_Harmonized'.lower()
harmonized_db_prod = 'CDW_Harmonized'.lower()

#current_dt = datetime.now()
#dt_sql_format = current_dt.strftime("%Y-%m-%d %H:%M:%S")
orig_file_name = os.path.basename(key_name)
dt_sql_format = datetime.strptime(archive_datetime.rstrip('_'),'%Y%m%d_%H%M%S').strftime("%Y-%m-%d %H:%M:%S")

s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_access_secret)

def load_to_database_tables(df, server_name, db_name, user_id, pwd, table_name, table_schema = "dbo", truncate_flag = False):
    try:
        params = "mssql+pyodbc://" + user_id + ":"+ pwd + "@" + server_name + "/" + db_name + "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=no"
        
        print(params)
        db_params = urllib.parse.quote_plus(params)
        engine = sqlalchemy.create_engine(params, encoding='utf8')

        #codes below come from https://medium.com/analytics-vidhya/speed-up-bulk-inserts-to-sql-db-using-pandas-and-python-61707ae41990
        from sqlalchemy import event
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(connSqlServer, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        conn = engine.connect()

        print("step 1: " + datetime.today().strftime('%Y%m%d %H:%M:%S'))
        if truncate_flag == True:
            conn.execute("DELETE FROM " + table_schema + '.' + table_name)

        print("step 2: " + datetime.today().strftime('%Y%m%d %H:%M:%S'))

        df.to_sql(table_name,engine,index=false,if_exists="append",schema=table_schema)

        print("Done: " + datetime.today().strftime('%Y%m%d %H:%M:%S'))
        conn.close()

        engine.dispose()

        #asynchronous happening where the method was returning before the procedure was finished, which resulted in unpredictable results on the database. Putting a time.sleep(1) (or whatever the appropriate number is) right after the call fixed this.
        #time.sleep(1)
        return 'Success'
    
    except Exception as e:
        error_message = str(e)
        return error_message

def get_env(env):

    if (env == "dev"):
        server_name = sql_server_dev
        staging_db_name = staging_db_dev
    elif (env == 'qa'):
        server_name = sql_server_qa
        staging_db_name = staging_db_qa
    elif (env == 'prod'):
        server_name = sql_server_prod
        staging_db_name = staging_db_prod
    elif (env.endswith('_dev')):
        server_name = sql_server_dev
        staging_db_name = env + '_staging'
    else:
        server_name = 'na'
        staging_db_name = 'na'
    
    reference_db_name = staging_db_name.replace('_staging', '_reference')
    harmonized_db_name = staging_db_name.replace('_staging', '_harmonized')
    return (server_name, staging_db_name, reference_db_name, harmonized_db_name)

def create_sql_server_engine(conn_string):
    engine = sqlalchemy.create_engine(conn_string, encoding='utf8', fast_executemany = True)
    Session = sessionmaker(bind = engine)
    session = Session()
    return session

def get_s3_replicate_flag(object_name, conn_string):
    query_get_config_value = """
    IF OBJECT_ID('dbo.fnGetConfigJsonNodeValue', 'FN') IS NOT NULL
        SELECT dbo.fnGetConfigJsonNodeValue('S3ReplicateFlag', '""" + object_name + """')
    ELSE
        SELECT '0'

    """

    session = create_sql_server_engine(conn_string = conn_string)

    replicate_flag = session.execute(query_get_config_value).fetchone()[0]
    if (replicate_flag == '-1'):
        raise Exception("S3ReplicateFlag config value in config table is not in json format.")
    elif (replicate_flag == '-2'):
        raise Exception("S3ReplicateFlag config item does not exist in config table.")
    elif (replicate_flag == '-3'):
        raise Exception(object_name + " node does not exist in S3ReplicateFlag json string in config table.")
    session.close()
    
    return (replicate_flag)

def log_activity(log_session, dom_id, message, staging_db_name):
    process_status = "F"
    if message.lower() == "null":
        process_status = "S"
    
    #Log an entry in maint.S3_FILE_PROCESS_HISTORY. This needs be done across all 3 enironments if all_env_flag is True
    sql_cmd = """
        INSERT INTO """ + staging_db_name + """.maint.S3_FILE_PROCESS_HISTORY(
            [file_name]
            ,[s3_path]
            ,[process_timestamp]
            ,[process_status]
            ,[message]
            ,[dom_id]
            ,[s3_archive_path]
        )
        SELECT
            [file_name] = '""" + orig_file_name + """'
            ,[s3_path] = 's3://""" + bucket_name + """/""" + os.path.dirname(key_name) + """'
            ,[process_timestamp] = '""" + dt_sql_format + """'
            ,[process_status] = '""" + process_status + """'
            ,[message] = NULLIF('""" + message + """', 'NULL')
            ,[dom_id] = NULLIF('""" + dom_id + """', 'NA')
            ,[s3_archive_path] = '""" + archive_path + """'
    """
    log_session.execute(sql_cmd)
    log_session.close()

### Start of Validation Script ###
# Sql Server Connection

try:
    file_name = orig_file_name.lower()
    raise_db_error = True #If error can be sent out via SQL Server email, we don't need to report job failure.

    sql_cmd = """
    
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX)
        EXEC ##PROC_NAME## @body = @email_body OUTPUT

        INSERT INTO #Email
        SELECT @email_body

        """

    sql_cmd_email_config = """
        SELECT
            email_body
        FROM #Email

    """

    (server, staging_db_name, reference_db_name, harmonized_db_name) = get_env(env)
    
    if object_name == "sales target":
        table = "STG_1_TGT0201_TARGETS_VALIDATION"
        schema = "wrk"
        target_table_database = staging_db_name
        notification_value = "SalesTargetEmailNotification"
        proc_name = 'uspValidateSalesTargetCsv'
        sql_cmd = sql_cmd.replace('##PROC_NAME##', proc_name)
        dom_id = '201'

    elif object_name == "npi":
        table = "Npi"
        schema = "wrk"
        target_table_database = reference_db_name
        notification_value = "NpiEmailNotification"
        proc_name = ''
        sql_cmd = """
        
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX)
        EXEC ##staging_db_name##.dbo.uspValidateNpi @body = @email_body OUTPUT

        IF @email_body = 'success'
        BEGIN
            BEGIN TRY
                BEGIN TRAN
                    TRUNCATE TABLE dbo.npi
                    ALTER TABLE wrk.npi SWITCH TO dbo.npi
                COMMIT
            END TRY
            BEGIN CATCH
                IF XACT_STATE() <> 0
                    ROLLBACK TRAN

                SET @email_body = ERROR_MESSAGE()
            END CATCH
        END

        INSERT INTO #Email
        SELECT @email_body        
        
        """
        dom_id = 'NA'

    elif object_name == "product_org_hierarchy":
        table = "Product_Org_Hierarchy"
        schema = "wrk"
        target_table_database = reference_db_name
        notification_value = "POHEmailNotification"
        proc_name = ''
        sql_cmd = """
        
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE ##reference_db_name##.dbo.Product_Org_Hierarchy
                ALTER TABLE ##reference_db_name##.wrk.Product_Org_Hierarchy SWITCH TO ##reference_db_name##.dbo.Product_Org_Hierarchy
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body
        
        """
        dom_id = 'NA'

    elif object_name == "aop":
        table = "STG_1_AOP0302_VALIDATION"
        schema = "wrk"
        target_table_database = staging_db_name
        notification_value = "AopEmailNotification"
        proc_name = 'uspValidateAop'
        sql_cmd = sql_cmd.replace('##PROC_NAME##', proc_name)
        dom_id = '302'

    elif object_name == "cmr":
        table = "STG_1_CMR0301_VALIDATION"
        schema = "wrk"
        target_table_database = staging_db_name
        notification_value = "CmrEmailNotification"
        proc_name = 'uspValidateCmr'
        sql_cmd = sql_cmd.replace('##PROC_NAME##', proc_name)
        dom_id = '301'

    elif object_name == "rraft":
        table = "STG_1_RRAFT0303_VALIDATION"
        schema = "wrk"
        target_table_database = staging_db_name
        notification_value = "RraftEmailNotification"
        proc_name = 'uspValidateRraft'
        sql_cmd = sql_cmd.replace('##PROC_NAME##', proc_name)
        dom_id = '303'

    elif object_name == "jabil":
        table = "JABIL_VALIDATION"
        schema = "dbo"
        target_table_database = harmonized_db_name
        notification_value = "JabilEmailNotification"
        proc_name = ''
        sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE dbo.JABIL
                ALTER TABLE dbo.JABIL_VALIDATION SWITCH TO dbo.JABIL
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """
        dom_id = 'NA'

    elif object_name == "lead_time":
        table = "DEFAULT_LEAD_TIME_VALIDATION"
        schema = "dbo"
        target_table_database = harmonized_db_name
        notification_value = "LeadTimeEmailNotification"
        proc_name = ''
        sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE dbo.DEFAULT_LEAD_TIME
                ALTER TABLE dbo.DEFAULT_LEAD_TIME_VALIDATION SWITCH TO dbo.DEFAULT_LEAD_TIME
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """
        dom_id = 'NA'

    elif object_name == "comm_product_class" or object_name == "comm_product_class_aig":
        if object_name == "comm_product_class_aig":
            table = "Comm_Product_Class_AIG"
            notification_value = "CPC_AIGEmailNotification"
            sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE ##reference_db_name##.dbo.Comm_Product_Class_AIG
                ALTER TABLE ##reference_db_name##.wrk.Comm_Product_Class_AIG SWITCH TO ##reference_db_name##.dbo.Comm_Product_Class_AIG
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """

        elif object_name == "comm_product_class":
            table = "Comm_Product_Class"
            notification_value = "CPCEmailNotification"
            sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE ##reference_db_name##.dbo.Comm_Product_Class
                ALTER TABLE ##reference_db_name##.wrk.Comm_Product_Class SWITCH TO ##reference_db_name##.dbo.Comm_Product_Class
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """
        schema = "wrk"
        target_table_database = reference_db_name
        #The sql command below is assumed to be executed under staging db, along with other validation procedures
        #Thus we need to specify reference db in the script
        proc_name = ''
        dom_id = 'NA'
        
    elif object_name == "duns_xref":
        table = "Duns_Xref"
        notification_value = "DunsEmailNotification"
        sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE ##reference_db_name##.dbo.Duns_Xref
                ALTER TABLE ##reference_db_name##.wrk.Duns_Xref SWITCH TO ##reference_db_name##.dbo.Duns_Xref
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """
        schema = "wrk"
        target_table_database = reference_db_name
        #The sql command below is assumed to be executed under staging db, along with other validation procedures
        #Thus we need to specify reference db in the script
        proc_name = ''
        dom_id = 'NA'
          
    elif object_name == "currency_fixed_rate":
        table = "currency_fixed_rate"
        notification_value = "CurrencyEmailNotification"
        sql_cmd = """
        SET NOCOUNT ON; 
        DROP TABLE IF EXISTS #Email
        
        CREATE TABLE #Email(
            email_body NVARCHAR(MAX)
        )
        
        DECLARE @email_body NVARCHAR(MAX) = 'Success'

        BEGIN TRY
            BEGIN TRAN
                TRUNCATE TABLE ##reference_db_name##.dbo.Currency_Fixed_Rate
                ALTER TABLE ##reference_db_name##.wrk.Currency_Fixed_Rate SWITCH TO ##reference_db_name##.dbo.Currency_Fixed_Rate
            COMMIT
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRAN

            SET @email_body = ERROR_MESSAGE()
        END CATCH

        INSERT INTO #Email
        SELECT @email_body

        """
        schema = "wrk"
        target_table_database = reference_db_name
        #The sql command below is assumed to be executed under staging db, along with other validation procedures
        #Thus we need to specify reference db in the script
        proc_name = ''
        dom_id = 'NA'
              
    else:
        table = ""
        notification_value = "EmailNotification"
        sql_cmd = ""
        sql_cmd_email_config = ""
        result = object_name + " with bucket name '" + bucket_name + "' and path in " + key_name + " is not matched to any validation procedure. "
        result += 'Validated files should be one in the list ("CAD Sales Rep Targets.csv", "npi.csv", "aop.csv", "CMR_extract.csv", "comm_product_class.csv"), case insensitive. '
        result += 'Prefix can be added to the file name, like dev_(validation in Dev server, dev databases)/qa_(validation in QA server, qa databases)/dh_CBCR_100_dev_ (validation in Dev server dh_CBCR_100 databases). '
        result += 'Prefix cannot be like prod_.'
        raise_db_error = False
        raise Exception(result)

    conn_string = "mssql+pyodbc://" + cad_user_id + ":"+ cad_user_pwd + "@" + server + "/" + staging_db_name + "?driver=ODBC+Driver+18+for+SQL+Server&autocommit=true&Encrypt=no"

    # If S3ReplicateFlag has been disabled, an error will be reported    
    if (get_s3_replicate_flag(object_name, conn_string)) != "1":
        message = "Replication " + object_name + " has been disabled in " + env + " environment. To enable uploading, please update S3ReplicateFlag value in config table."
        raise Exception(message)

    conn_string = "mssql+pyodbc://" + cad_user_id + ":"+ cad_user_pwd + "@" + server + "/" + target_table_database + "?driver=ODBC+Driver+18+for+SQL+Server&autocommit=true&Encrypt=no"
    print(conn_string)
    session = create_sql_server_engine(conn_string = conn_string)

    response = s3.get_object(Bucket=bucket_name, Key=archive_path)
    
    contents = response['Body'].read()
    try:
        df = pandas.read_csv(io.BytesIO(contents), thousands=',', encoding='utf-8', engine='python', dtype = str)
    except Exception:
        df = pandas.read_csv(io.BytesIO(contents), thousands=',', encoding='latin1', engine='python', dtype = str)

    if (table != ""):
        result = load_to_database_tables(df = df, server_name = server, db_name = target_table_database, user_id = cad_user_id, pwd = cad_user_pwd, table_name = table, table_schema = schema, truncate_flag = True)
        if result.lower() != 'success':
            raise Exception(result)

    if (sql_cmd != ""):
        result = ""
        if proc_name == '':
            session.execute(sql_cmd.replace("##reference_db_name##", reference_db_name).replace("##staging_db_name##", staging_db_name))
            session.close()
            if (sql_cmd_email_config != ""):
                result = session.execute(sql_cmd_email_config).fetchone()[0]
                session.close()
        else:
            conn_string = "mssql+pyodbc://" + cad_user_id + ":"+ cad_user_pwd + "@" + server + "/" + staging_db_name + "?driver=ODBC+Driver+18+for+SQL+Server&autocommit=true&Encrypt=no"
            print(conn_string)
            session_validation = create_sql_server_engine(conn_string = conn_string)
            session_validation.execute(sql_cmd)
            if (sql_cmd_email_config != ""):
                result = session_validation.execute(sql_cmd_email_config).fetchone()[0]
            session_validation.close()


    print(result)

    if result.lower() == "success" or result.lower().startswith("warning"):
        #The message will be overwritten if all_env_flag is True
        if result.lower().startswith("warning"):
            validation_result = "<html><body>" + result + "</body></html>"
            email_body = '<html><body>Data in file has been loaded to ' + server + ' environment ' + target_table_database + ' database.</body></html>'
        else:
            validation_result = ''
            email_body = '<html><body>Data validation passed! Data in file has been loaded to ' + server + ' environment ' + target_table_database + ' database.</body></html>'

        email_body += validation_result
    
    #All files will be archived
    message = 'NULL'
    archive_status = log_activity(session, dom_id, message, staging_db_name)

    if result.lower() == "success" or result.lower().startswith("warning"):
        if result.lower().startswith("warning"):
            email_subject = orig_file_name + ' Validation Succeeded with Warnings'
        else:
            email_subject = orig_file_name + ' Validation Succeeded'
        sql_cmd = """
            SET NOCOUNT ON; 
            
            DECLARE @profile_name VARCHAR(100) = CONCAT(@@SERVERNAME, '_MailProfile')
                    ,@email_body NVARCHAR(MAX) = N'""" + email_body + """'
                    ,@email_list VARCHAR(1000) = """ + staging_db_name + """.dbo.fnGetConfigValue('""" + notification_value + """')
                    ,@email_subject VARCHAR(1000) = '""" + email_subject + """'

            EXEC msdb.dbo.sp_send_dbmail 
                @profile_name = @profile_name
                ,@recipients = @email_list
                ,@subject = @email_subject
                ,@body = @email_body
                ,@body_format ='HTML'
        
        """
        session.execute(sql_cmd)
        session.close()

    else:
        raise_db_error = False
        raise Exception(result)


except Exception as e:
    error_message = str(e).replace("'", "''")
    conn_string = "mssql+pyodbc://" + cad_user_id + ":"+ cad_user_pwd + "@" + server + "/" + staging_db_name + "?driver=ODBC+Driver+18+for+SQL+Server&autocommit=true&Encrypt=no"
    engine = sqlalchemy.create_engine(conn_string, encoding='utf8', fast_executemany = True)
    Session = sessionmaker(bind = engine)
    session = Session()

    if 'result' in locals():
        if result.lower() == 'success':
            error_message = orig_file_name + " passed validation but validation script has encountered an issue. " + error_message

        sql_cmd = """
            SET NOCOUNT ON; 
            
            DECLARE @profile_name VARCHAR(100) = CONCAT(@@SERVERNAME, '_MailProfile')
                    ,@email_body NVARCHAR(MAX) = N'""" + error_message + """'
                    ,@email_list VARCHAR(1000) = dbo.fnGetConfigValue('""" + notification_value + """')
                    ,@email_subject VARCHAR(1000) = '""" + orig_file_name+ """ Validation Failed'

            EXEC msdb.dbo.sp_send_dbmail 
                @profile_name = @profile_name
                ,@recipients = @email_list
                ,@subject = @email_subject
                ,@body = @email_body
                ,@body_format ='HTML'

            """
        print(sql_cmd)
        session.execute(sql_cmd)
        raise_db_error = False
        session.close()
    
    if raise_db_error:
        raise Exception(e)

