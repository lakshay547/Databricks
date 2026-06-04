# Databricks notebook source
# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %run ./ReusableFunctions

# COMMAND ----------

load_type = "initial"
project = "rsd_eu_reportwatcher_analytics"
jobID = "632039096394046"
load_group = "6"
src_sys_cd = "ibs"
tableslist = None
try:
  tableslist = "('A02212 Holding Extract 02','A02212 Holding Extract 03','A02212 Holding Extract 05','A02212 Holding Extract 06','A02212 Holding Extract 07','A02212 Holding Extract 08','A02212 Holding Extract 09','A02212 Holding Extract 10','A02212 Holding Extract 11','A02212 Holding Extract 12','A02212 Holding Extract 14','A02212 Holding Extract 15','A02212 Holding Extract 16','A02212 Holding Extract 17','A02212 Holding Extract 18')"
except:
  print("No tables list is provided")
environment = "prod"
config_unity_catalog = "ccg_raw.config"
raw_unity_catalog = "ccg_raw.rsd_eu_ibs"
#if log_check is true, this job will check respective table logs success status for the day and if respective table didnt ran or it gor failed, then it will trigger the respective table notebook, else it will ignore. If log_check is false, directly, it will trigger the respective table notebook.
log_check = "false"
try:
  log_check = dbutils.widgets.get("log_check")
except:
  print("No log_check is provided")

job_type = "data_load"
try:
  job_type = dbutils.widgets.get("job_type")
except:
  print("No job_type is provided")

full_load = "false"
try:
  full_load = dbutils.widgets.get("full_load")
except:
  print("No full_load is provided")

lookback_days = "1"
try:
  lookback_days = dbutils.widgets.get("lookback_days")
except:
  print("No lookback_days is provided")

table_overwrite = "false"
try:
  table_overwrite = dbutils.widgets.get("table_overwrite")
except:
  print("No table_overwrite is provided")

refresh_type = "REG"
try:
  refresh_type = dbutils.widgets.get("refresh_type")
except:
  print("refresh_type is not provided")


if full_load == "false" and table_overwrite == "true":
  raise Exception("table cannot be overwrite when full load is false")

# COMMAND ----------

if not project or not load_type or not load_group:
      raise Exception("Make sure project, load_type, load_group are specified")

if tableslist is None:
      query = f"select * from {config_unity_catalog}.{cl_control_table} where src_sys_cd = '{src_sys_cd}' and load_group = '{load_group}' and project = '{project}' and is_active = 'Y' and (pre_proc_script_path is not null or pre_proc_script_path != 'null')"
else:
      query = f"select * from {config_unity_catalog}.{cl_control_table} where src_sys_cd = '{src_sys_cd}' and load_group = '{load_group}' and project = '{project}' and table_name in {tableslist} and is_active = 'Y' and (pre_proc_script_path is not null or pre_proc_script_path != 'null')"

print(query)
lookupDF = spark.sql(query)
if job_type == 'optimize':
      lookupDF = lookupDF.filter((lower(lookupDF.optimize) == 'true') & (lower(lookupDF.vaccum) == 'true'))

# COMMAND ----------

def create_job_parameters(item):
  job_parameters = ast.literal_eval(item['pre_proc_args'])
  job_parameters["unity_catalog_name"] = item['unity_catalog_name']
  job_parameters["write_format"] = item['write_format']
  job_parameters["table_name"] = item['table_name']
  job_parameters['project'] = project
  job_parameters['load_type'] = load_type
  job_parameters['partition'] = job_parameters.pop('target_table_partition_columns')
  job_parameters['unity_catalog'] = job_parameters.pop('unity_catalog_name')
  if 'full_load' in job_parameters.keys():
    print("full load is configured in control table")
  else:
    job_parameters['full_load'] = item['full_load']
  if 'lookback_days' in job_parameters.keys():
    print("lookback days is configured in control table")
  else:
    job_parameters['lookback_days'] = item['lookback_days']
  job_parameters['table_overwrite'] = item['table_overwrite']
  job_parameters['refresh_type'] = item['refresh_type']
  

  return job_parameters

# COMMAND ----------

def read_log_status(table_name, project, load_type, unity_catalog):
  log_df = spark.sql(f"select * from {config_unity_catalog}.{run_stats_table} where project = '{project}' and load_type = '{load_type}' and table_name like '{unity_catalog}.{table_name}' and to_date(run_stat_ts) = to_date(now()) order by run_stat_ts desc limit 1;")

  if log_df.count() == 1:
    log_data = log_df.collect()[0]
    if job_type == 'data_load' and (log_data['status'] == 'success' or log_data['status'] == 'Optimization Process Completed'):
      return 1
    elif job_type == 'optimize' and log_data['status'] == 'Optimization Process Completed':
      return 1
  
  return 0


# COMMAND ----------

def run_notebook(notebook,timeout,job_parameter):
  global failed
  table_name = job_parameter["table_name"]
  if log_check.lower() == "true":
    read_log_status_val = read_log_status(table_name,job_parameter['project'],job_parameter['load_type'],job_parameter['unity_catalog'])
    if read_log_status_val == 1:
      print(f"{table_name} -> load already completed\n")
    else:
      print(f"{table_name} -> load started\n")
      print(dbutils.notebook.run(notebook,timeout,job_parameter))
  else:
    print(f"{table_name} -> load started\n")
    print(dbutils.notebook.run(notebook,timeout,job_parameter))

# COMMAND ----------

lookupDF = lookupDF.select('table_name','pre_proc_args','pre_proc_script_path','unity_catalog_name','write_format')
lookupDF = lookupDF.withColumn("full_load", lit(full_load)) \
  .withColumn("lookback_days", lit(lookback_days)) \
    .withColumn("table_overwrite", lit(table_overwrite)) \
    .withColumn("refresh_type", lit(refresh_type))
from concurrent.futures import ThreadPoolExecutor, wait


timeout = 0
try:  
  if lookupDF.count() > 0:                        
    tables_info_tuple = lookupDF.toPandas().to_dict('records')
    
    if job_type == 'optimize':
      RunParallel = lambda tables_info: run_notebook('CCG_RSDEU_CL_OPTIMIZE', timeout, create_job_parameters(tables_info))
    elif job_type == 'data_load':
      RunParallel = lambda tables_info: run_notebook(tables_info['pre_proc_script_path'].replace('notebook://',''), timeout, create_job_parameters(tables_info))
    
    NumberOfParalellThreads = len(tables_info_tuple)

    pool = ThreadPoolExecutor(NumberOfParalellThreads)

    results = []

    with ThreadPoolExecutor(NumberOfParalellThreads) as pool:
        results.extend(pool.map(RunParallel, tables_info_tuple))
  else:
    print("No table to process.")
except Exception as e:
    raise e