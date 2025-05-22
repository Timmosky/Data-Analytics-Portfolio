import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node logdata
logdata_node1747670556066 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://pqlogstext/PQobjects/"], "recurse": True}, transformation_ctx="logdata_node1747670556066")

# Script generated for node ibdata
ibdata_node1747670561560 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://dataibdrive/iboutput/"], "recurse": True}, transformation_ctx="ibdata_node1747670561560")

# Script generated for node Log_Change_Schema
Log_Change_Schema_node1747670700787 = ApplyMapping.apply(frame=logdata_node1747670556066, mappings=[("tar_systemid", "string", "tar_systemid", "varchar"), ("model_type", "string", "model_type", "string"), ("log_id", "string", "log_id", "string"), ("log_text", "string", "log_text", "string"), ("log_timestamp_ts", "string", "log_timestamp_ts", "string"), ("msg_date", "string", "msg_date", "string"), ("file_key", "string", "file_key", "string")], transformation_ctx="Log_Change_Schema_node1747670700787")

# Script generated for node Ib_Change_Schema
Ib_Change_Schema_node1747670707016 = ApplyMapping.apply(frame=ibdata_node1747670561560, mappings=[("systemid", "string", "systemid", "varchar"), ("customername", "string", "customername", "string"), ("age", "string", "age", "string"), ("site_name", "string", "site_name", "string"), ("region", "string", "region", "string"), ("grp_of_countries_description", "string", "grp_of_countries_description", "string"), ("country", "string", "country", "string")], transformation_ctx="Ib_Change_Schema_node1747670707016")

# Script generated for node Merged Query
SqlQuery306 = '''
select * from log left join ib on log.tar_systemid = ib.systemid
'''
MergedQuery_node1747670917794 = sparkSqlQuery(glueContext, query = SqlQuery306, mapping = {"log":Log_Change_Schema_node1747670700787, "ib":Ib_Change_Schema_node1747670707016}, transformation_ctx = "MergedQuery_node1747670917794")

# Script generated for node Transformed Table
SqlQuery307 = '''
with system as (SELECT
            tar_systemid, customername, site_name, file_key,
            msg_date,
            log_text,
            model_type,
            log_timestamp_ts,
            REGEXP_EXTRACT(log_text, 'Drive [A-Z]', 0) AS drive_id,
            ROW_NUMBER() OVER (PARTITION BY tar_systemid, cast(msg_date as date), REGEXP_EXTRACT(log_text, 'Drive [A-Z]', 0) ORDER BY log_timestamp_ts DESC) AS rn
        FROM
            myDataSource
        WHERE
            (model_type LIKE '%E10%'
            OR model_type LIKE '%FORTIS%')
            AND log_text LIKE 'Drive%')
            -------AND tar_systemid NOT IN (SELECT tar_systemid FROM "us_prod_magellan_ul"."ul_logdb_mainlog_all" WHERE cast(msg_date as date) > current_date)
    select tar_systemid,msg_date,customername, site_name, file_key,
     MAX(CASE when drive_id = 'Drive C' then (select(REGEXP_EXTRACT(log_text, '(\\\\d+)%')))END) as DRIVE_C, 
       MAX(CASE when drive_id = 'Drive D' then (select(REGEXP_EXTRACT(log_text, '(\\\\d+)%')))END) as DRIVE_D, 
       MAX(CASE when drive_id = 'Drive E' then (select(REGEXP_EXTRACT(log_text, '(\\\\d+)%')))END) as DRIVE_E, 
      ---------- MAX(CASE when drive_id = 'Drive V' then (select(REGEXP_EXTRACT(log_text, '(\\\\d+)%')))END) as DRIVE_V, 
      MAX(CASE when drive_id = 'Drive Z' then (select(REGEXP_EXTRACT(log_text, '(\\\\d+)%')))END) as DRIVE_Z
        from system
       where rn = 1
       group by tar_systemid, file_key, msg_date, customername, site_name order by tar_systemid, msg_date desc;
'''
TransformedTable_node1747671037241 = sparkSqlQuery(glueContext, query = SqlQuery307, mapping = {"myDataSource":MergedQuery_node1747670917794}, transformation_ctx = "TransformedTable_node1747671037241")

# Script generated for node Change Schema
ChangeSchema_node1747673028064 = ApplyMapping.apply(frame=TransformedTable_node1747671037241, mappings=[("tar_systemid", "string", "tar_systemid", "varchar"), ("msg_date", "string", "msg_date", "varchar"), ("customername", "string", "customername", "string"), ("site_name", "string", "site_name", "string"), ("file_key", "string", "file_key", "string"), ("DRIVE_C", "string", "DRIVE_C", "varchar"), ("DRIVE_D", "string", "DRIVE_D", "varchar"), ("DRIVE_E", "string", "DRIVE_E", "varchar"), ("DRIVE_Z", "string", "DRIVE_Z", "varchar")], transformation_ctx="ChangeSchema_node1747673028064")

# Script generated for node Staging table
Stagingtable_node1747673092687 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1747673028064, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO public.pqbench USING public.pqbench_temp_57xse6 ON pqbench.tar_systemid = pqbench_temp_57xse6.tar_systemid AND pqbench.msg_date = pqbench_temp_57xse6.msg_date WHEN MATCHED THEN UPDATE SET tar_systemid = pqbench_temp_57xse6.tar_systemid, msg_date = pqbench_temp_57xse6.msg_date, customername = pqbench_temp_57xse6.customername, site_name = pqbench_temp_57xse6.site_name, file_key = pqbench_temp_57xse6.file_key, DRIVE_C = pqbench_temp_57xse6.DRIVE_C, DRIVE_D = pqbench_temp_57xse6.DRIVE_D, DRIVE_E = pqbench_temp_57xse6.DRIVE_E, DRIVE_Z = pqbench_temp_57xse6.DRIVE_Z WHEN NOT MATCHED THEN INSERT VALUES (pqbench_temp_57xse6.tar_systemid, pqbench_temp_57xse6.msg_date, pqbench_temp_57xse6.customername, pqbench_temp_57xse6.site_name, pqbench_temp_57xse6.file_key, pqbench_temp_57xse6.DRIVE_C, pqbench_temp_57xse6.DRIVE_D, pqbench_temp_57xse6.DRIVE_E, pqbench_temp_57xse6.DRIVE_Z); DROP TABLE public.pqbench_temp_57xse6; END;", "redshiftTmpDir": "s3://aws-glue-assets-435557266949-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "public.pqbench_temp_57xse6", "connectionName": "Redshiffpqconnect", "preactions": "CREATE TABLE IF NOT EXISTS public.pqbench (tar_systemid VARCHAR, msg_date VARCHAR, customername VARCHAR, site_name VARCHAR, file_key VARCHAR, DRIVE_C VARCHAR, DRIVE_D VARCHAR, DRIVE_E VARCHAR, DRIVE_Z VARCHAR); DROP TABLE IF EXISTS public.pqbench_temp_57xse6; CREATE TABLE public.pqbench_temp_57xse6 (tar_systemid VARCHAR, msg_date VARCHAR, customername VARCHAR, site_name VARCHAR, file_key VARCHAR, DRIVE_C VARCHAR, DRIVE_D VARCHAR, DRIVE_E VARCHAR, DRIVE_Z VARCHAR);"}, transformation_ctx="Stagingtable_node1747673092687")

job.commit()