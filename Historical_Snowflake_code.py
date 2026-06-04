# Databricks notebook source
# =============================================================================
# MULTI-TABLE LOADING CONFIGURATION
# =============================================================================
# 
# CONFIGURATION OPTIONS:
# 1. SINGLE TABLE MODE: Set TABLE_NAME and leave TABLE_NAMES empty
# 2. MULTIPLE TABLES MODE: Set TABLE_NAMES list and leave TABLE_NAME empty
# 3. ALL TABLES MODE: Set LOAD_ALL_TABLES = True (loads all tables from Table_Info.json)
#
# All other configurations will be automatically loaded from JSON files
# =============================================================================

# =============================================================================
# INPUT PARAMETERS - MODIFY THESE
# =============================================================================

ORG_NAME = "DEBT_SCHEMA"                    # Organization name from Org_Details.json
SOURCE_DATABASE = "etl_accord_us2"
         # Source database name

# SINGLE TABLE MODE - Load one specific table
TABLE_NAME = ""                              # Leave empty if using multiple tables mode

# MULTIPLE TABLES MODE - Load specific tables
# TABLE_NAMES = ["monitoring_73_vc_tool","monitoring_73_vc_product_tools","monitoring_73_vc_user_product_tools"]

TABLE_NAMES = [
    "credit_73_cashflow",
    "credit_73_cashflow_schedule",
    "credit_73_concluded_warrant",
    "credit_73_concluded_warrant_range_analysis",
"credit_73_debt_model",
"credit_73_discount_adjustment_buildup",
"credit_73_discount_adjustment_calibration",
"credit_73_discount_rate_computation_build_up",
"credit_73_discount_rate_computation_calibration",
"credit_73_discount_rate_concluded",
"credit_73_discount_rate_lookup",
"credit_73_discount_rate_range_analysis",
"credit_73_general_details",
"credit_73_look_up_debt_model",
"credit_73_look_up_metadata",
"credit_73_multiple_based_warrant",
"credit_73_multiple_based_warrant_range",
"credit_73_principal_outstanding",
"credit_73_principal_outstanding_schedule",
"credit_73_valuation_approach",
"credit_73_valuation_conclusion",
"credit_73_saf_form_debt_company",
"credit_73_debt_model_inputs",
"credit_73_annual_historical_financial",
"credit_73_annual_projected_financial"
]

# ALL TABLES MODE - Load all tables from Table_Info.json
LOAD_ALL_TABLES = False                      # Set to True to load all tables

# =============================================================================
# CONFIGURATION PATHS
# =============================================================================

org_details_path = f'/Workspace/Shared/etl_databricks/snowflake/Org_Details_CREDIT.json'
table_info_path = f'/Workspace/Shared/etl_databricks/snowflake/Table_Info_CREDIT.json'
org_version_table_name = "etl_accord_us2._internal.org_version_tracking"

# =============================================================================
# HARDCODED TABLES (Tables that don't require org filtering)
# =============================================================================

HARDCODED_TABLES = [
 
 

]

# =============================================================================
# VALIDATION AND MODE DETECTION
# =============================================================================

def determine_loading_mode():
    """Determine which loading mode to use based on configuration"""
    if LOAD_ALL_TABLES:
        return "ALL_TABLES"
    elif TABLE_NAME and not TABLE_NAMES:
        return "SINGLE_TABLE"
    elif TABLE_NAMES and not TABLE_NAME:
        return "MULTIPLE_TABLES"
    else:
        raise ValueError("Invalid configuration: Must specify either TABLE_NAME, TABLE_NAMES, or set LOAD_ALL_TABLES=True")

LOADING_MODE = determine_loading_mode()

print("✅ Configuration parameters set:")
print(f"   Organization: {ORG_NAME}")
print(f"   Source Database: {SOURCE_DATABASE}")
print(f"   Loading Mode: {LOADING_MODE}")
if LOADING_MODE == "SINGLE_TABLE":
    print(f"   Table Name: {TABLE_NAME}")
elif LOADING_MODE == "MULTIPLE_TABLES":
    print(f"   Table Names: {TABLE_NAMES}")
elif LOADING_MODE == "ALL_TABLES":
    print(f"   Loading: All tables from Table_Info.json")
print(f"   JSON Config Paths: {org_details_path}, {table_info_path}")

# COMMAND ----------

import json
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import unhex, when, col,lit
from pyspark.sql.types import BinaryType

# =============================================================================
# CONFIGURATION LOADING AND LOOKUP FUNCTIONS
# =============================================================================

def load_json_configs():
    """Load configuration from JSON files"""
    print("=== LOADING CONFIGURATIONS FROM JSON FILES ===")
    
    # Load organization details
    if not os.path.exists(org_details_path):
        raise FileNotFoundError(f"Organization details file not found: {org_details_path}")
    
    with open(org_details_path, 'r') as f:
        orgs_json = json.load(f)
    
    # Load table information
    if not os.path.exists(table_info_path):
        raise FileNotFoundError(f"Table information file not found: {table_info_path}")
    
    with open(table_info_path, 'r') as f:
        table_info_json = json.load(f)
    
    print(f"✅ Loaded organization details from: {org_details_path}")
    print(f"✅ Loaded table information from: {table_info_path}")
    
    return orgs_json, table_info_json

def find_organization_config(orgs_json, org_name):
    """Find organization configuration by name"""
    for org in orgs_json.get("list_orgs", []):
        if org["orgname"].upper() == org_name.upper():
            return {
                "orgid": org["orgid"],
                "orgname": org["orgname"],
                "destination": org.get("destination", "snowflake").lower(),
                "config": org.get("config", org.get("snowflakeconfig", org.get("mssqlconfig", {})))
            }
    
    available_orgs = [org["orgname"] for org in orgs_json.get("list_orgs", [])]
    raise ValueError(f"Organization '{org_name}' not found. Available organizations: {available_orgs}")

def find_table_config(table_info_json, table_name):
    """Find table configuration by name"""
    for table_info in table_info_json:
        if table_info["tablename"].lower() == table_name.lower():
            return {
                "tablename": table_info["tablename"],
                "orgcolumnname": table_info["orgcolumnname"],
                "primaryKey": table_info["primaryKey"],
                "mappingDetails": table_info.get("mappingDetails", "")
            }
    
    available_tables = [table["tablename"] for table in table_info_json]
    raise ValueError(f"Table '{table_name}' not found. Available tables: {available_tables}")

def get_tables_to_load(table_info_json):
    """Get list of tables to load based on configuration mode"""
    if LOADING_MODE == "SINGLE_TABLE":
        return [TABLE_NAME]
    elif LOADING_MODE == "MULTIPLE_TABLES":
        return TABLE_NAMES
    elif LOADING_MODE == "ALL_TABLES":
        return [table["tablename"] for table in table_info_json]
    else:
        raise ValueError(f"Unknown loading mode: {LOADING_MODE}")

def validate_tables_exist(table_info_json, tables_to_load):
    """Validate that all specified tables exist in Table_Info.json"""
    available_tables = [table["tablename"] for table in table_info_json]
    missing_tables = [table for table in tables_to_load if table not in available_tables]
    
    if missing_tables:
        raise ValueError(f"Tables not found in Table_Info.json: {missing_tables}")
    
    return True

def validate_and_setup_configuration():
    """Validate input parameters and load configurations"""
    print("=== VALIDATING INPUT PARAMETERS ===")
    
    # Validate input parameters
    if not ORG_NAME:
        raise ValueError("ORG_NAME must be specified")
    if not SOURCE_DATABASE:
        raise ValueError("SOURCE_DATABASE must be specified")
    
    print(f"✅ Input parameters validated")
    print(f"   Organization: {ORG_NAME}")
    print(f"   Source Database: {SOURCE_DATABASE}")
    print(f"   Loading Mode: {LOADING_MODE}")
    
    # Load JSON configurations
    orgs_json, table_info_json = load_json_configs()
    
    # Get tables to load
    tables_to_load = get_tables_to_load(table_info_json)
    print(f"   Tables to load: {tables_to_load}")
    
    # Validate tables exist
    validate_tables_exist(table_info_json, tables_to_load)
    print(f"✅ All specified tables found in Table_Info.json")
    
    # Find organization configuration
    print(f"\n🔍 Looking up organization: {ORG_NAME}")
    org_config = find_organization_config(orgs_json, ORG_NAME)
    print(f"✅ Found organization: {org_config['orgname']} (ID: {org_config['orgid']})")
    print(f"✅ Destination: {org_config['destination'].upper()}")
    
    # Set up destination configuration
    destination_config = org_config['config'].copy()
    if org_config['destination'] == "snowflake":
        destination_config["sfSchema"] = org_config['orgname']
    elif org_config['destination'] == "mssql":
        if "driver" not in destination_config:
            destination_config["driver"] = "ODBC Driver 17 for SQL Server"
        if "port" not in destination_config:
            destination_config["port"] = "1433"
    
    # Set global variables for use in other functions
    global TARGET_ORG_ID, TARGET_ORG_NAME, DESTINATION, SOURCE_CATALOG, TABLES_TO_LOAD,TABLE_INFO_JSON,config
    
    TARGET_ORG_ID = org_config['orgid']
    TARGET_ORG_NAME = org_config['orgname']
    DESTINATION = org_config['destination']
    SOURCE_CATALOG = SOURCE_DATABASE
    TABLE_INFO_JSON=table_info_json
    TABLES_TO_LOAD = tables_to_load
    config = destination_config
    
    print(f"\n✅ Configuration setup complete:")
    print(f"   Source Catalog: {SOURCE_CATALOG}")
    print(f"   Target Org: {TARGET_ORG_NAME} (ID: {TARGET_ORG_ID})")
    print(f"   Destination: {DESTINATION.upper()}")
    print(f"   Tables to Process: {len(TABLES_TO_LOAD)} tables")
    
    if DESTINATION == "snowflake":
        print(f"   Snowflake URL: {config.get('sfUrl', 'Not configured')}")
        print(f"   Database: {config.get('sfDatabase', 'Not configured')}")
        print(f"   Schema: {config.get('sfSchema', 'Not configured')}")
    elif DESTINATION == "mssql":
        print(f"   MSSQL Server: {config.get('server', 'Not configured')}")
        print(f"   Database: {config.get('database', 'Not configured')}")
    
    return config

def check_source_tables_exist():
    """Check if all source tables exist"""
    print("=== VALIDATING SOURCE TABLES ===")
    missing_tables = []
    existing_tables = []
    
    for table_name in TABLES_TO_LOAD:
        try:
            table_name = table["tablename"]
            source_schema = table["source_schema"]
            source_table = f"{SOURCE_CATALOG}.{source_schema}.{table_name}"
            df = spark.read.table(source_table)
            record_count = df.count()
            existing_tables.append(table_name)
            print(f"✅ {table_name}: {record_count:,} records")
        except AnalysisException as e:
            missing_tables.append(table_name)
            print(f"❌ {table_name}: Table does not exist")
    
    if missing_tables:
        print(f"\n❌ Missing tables: {missing_tables}")
        return False
    else:
        print(f"\n✅ All {len(existing_tables)} tables exist and accessible")
        return True

# Execute configuration loading and validation
config = validate_and_setup_configuration()

# Check source tables existence
if not check_source_tables_exist():
    raise Exception("Source tables validation failed")

# Create version tracking table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {org_version_table_name} (
        table_name STRING,
        org_id STRING,
        last_version STRING,
        updatedAt TIMESTAMP
    )
""")

print(f"✅ Version tracking table ready: {org_version_table_name}")
print("=== CONFIGURATION LOADING COMPLETE ===")

# COMMAND ----------

# =============================================================================
# MULTI-TABLE LOADING FUNCTIONS
# =============================================================================

def load_single_table(table_name, table_config):
    """
    Load a single table from a specific schema of a specific database
    """

    source_schema = table_config["source_schema"]
    print(f"\n{'='*60}")
    print(f"LOADING TABLE: {table_name}")
    print(f"Source: {SOURCE_CATALOG}.{source_schema}.{table_name}")
    print(f"Target Org: {TARGET_ORG_NAME} (ID: {TARGET_ORG_ID})")
    print(f"Destination: {DESTINATION.upper()}")
    print(f"{'='*60}")
    
    source_table = f"{SOURCE_CATALOG}.{source_schema}.{table_name}"
    
    # Determine target table name
    if DESTINATION == "snowflake":
        full_target_table = f"{config['sfDatabase']}.{config['sfSchema']}.{table_name.upper()}"
    elif DESTINATION == "mssql":
        full_target_table = f"[{config['database']}].[GX_EMS].[{table_name.upper()}]"
    
    print(f"Target table: {full_target_table}")
    
    try:
        # Check if this is a hardcoded table (no org filtering)
        if table_name.lower() in [t.lower() for t in HARDCODED_TABLES]:
            print(f"📋 Processing as HARDCODED table (no org filtering)")
            df = spark.read.table(source_table)
            record_count = df.count()
            print(f"Total records in hardcoded table: {record_count:,}")
            
            # Add org_id column for tracking
            df = df.withColumn("org_id", F.lit(TARGET_ORG_ID))
            
        else:
            # Process with org filtering or mapping
            org_column = table_config.get("orgcolumnname", "").strip().lower()
            mapping_details = table_config.get("mappingDetails", {})
            
            print(f"📋 Processing with org filtering")
            print(f"Org column: {org_column}")
            
            if org_column and org_column != "none":
                # Direct org filtering
                print(f"Using direct org filtering on column: {org_column}")
                df = spark.read.table(source_table).filter(F.col(org_column) == TARGET_ORG_ID)
                record_count = df.count()
                print(f"Records for org {TARGET_ORG_ID}: {record_count:,}")
                
            elif org_column == "none" and mapping_details:
                # Use mapping table
                print(f"Using mapping table: {mapping_details['tablename']}")
                mapping_table_name = mapping_details["tablename"]
                mapping_org_column = mapping_details["orgcolumnname"]
                foreign_column = mapping_details["foreigncolumnname"]
                primary_column = mapping_details.get("primarycolumnname", foreign_column)
                
                # Get foreign keys for this org
                mapping_df = spark.table(f"{SOURCE_CATALOG}.{source_schema}.{mapping_table_name}")
                filtered_map_df = mapping_df.filter(F.col(mapping_org_column) == TARGET_ORG_ID)
                foreign_keys = [row[foreign_column] for row in filtered_map_df.select(foreign_column).distinct().collect()]
                
                if not foreign_keys:
                    raise Exception(f"No foreign keys for org '{TARGET_ORG_ID}' in mapping table '{mapping_table_name}'")
                
                print(f"Found {len(foreign_keys)} foreign keys for org {TARGET_ORG_ID}")
                
                # Filter source table using foreign keys
                source_df = spark.read.table(source_table)
                df = source_df.filter(F.col(primary_column).isin(foreign_keys))
                record_count = df.count()
                print(f"Records after mapping filter: {record_count:,}")
                
                # Add org_id column
                df = df.withColumn("org_id", F.lit(TARGET_ORG_ID))
                
            else:
                raise Exception(f"No valid org filtering strategy for table '{table_name}'")
        
        # Check if target table exists
        print(f"\n🔍 Checking if target table exists...")
        if DESTINATION == "snowflake":
            table_exists = check_snowflake_table_exists(config, table_name)
        elif DESTINATION == "mssql":
            table_exists = check_mssql_table_exists(config, table_name)
        
        if not table_exists:
            print(f"⚠️ Table '{full_target_table}' does not exist. It will be created.")
            ddl=spark_to_snowflake_ddl(df, full_target_table)
            print(ddl)
            execute_snowflake_ddl(ddl, config)
            print(f"Table '{full_target_table}' created.")
        else:
            print(f"✅ Table '{full_target_table}' exists. Data will be overwritten.")
            truncate_snowflake_table(full_target_table, config)
            print(f"Table '{full_target_table}' truncated.")
        
        # Display schema
        print(f"\n📊 Table Schema:")
        df.printSchema()
        
        # Write to destination
        print(f"\n💾 Writing data to {DESTINATION.upper()}...")

        if DESTINATION == "snowflake":
            df.write \
              .format("snowflake") \
              .options(**config) \
              .option("dbtable", full_target_table) \
              .mode("append") \
              .save()
        elif DESTINATION == "mssql":
            df = convert_complex_types_to_strings(df)
            connection_string = get_mssql_connection_string(config)
            df.write \
              .format("jdbc") \
              .option("url", connection_string) \
              .option("dbtable", f"[GX_EMS].[{table_name.upper()}]") \
              .mode("overwrite") \
              .save()
        
        print(f"✅ Successfully loaded {record_count:,} records to {full_target_table}")
        
        # Update version tracking
        update_version_tracking(source_table, TARGET_ORG_ID)
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading table '{table_name}': {e}")
        raise

def load_multiple_tables():
    """
    Load multiple tables based on configuration
    """
    print(f"\n{'='*80}")
    print(f"MULTI-TABLE LOADING PROCESS")
    print(f"Organization: {TARGET_ORG_NAME} (ID: {TARGET_ORG_ID})")
    print(f"Destination: {DESTINATION.upper()}")
    print(f"Tables to Process: {len(TABLES_TO_LOAD)}")
    print(f"{'='*80}")
    
    # Load table configurations
    with open(table_info_path, 'r') as f:
        table_info_json = json.load(f)
    
    # Create table config map
    table_config_map = {table["tablename"]: table for table in table_info_json}
    
    # Track results
    successful_tables = []
    failed_tables = []
    
    # Process each table
    for i, table_name in enumerate(TABLES_TO_LOAD, 1):
        print(f"\n{'='*80}")
        print(f"PROCESSING TABLE {i}/{len(TABLES_TO_LOAD)}: {table_name}")
        print(f"{'='*80}")
        
        try:
            table_config = table_config_map[table_name]
            success = load_single_table(table_name, table_config)
            
            if success:
                successful_tables.append(table_name)
                print(f"✅ Table {i}/{len(TABLES_TO_LOAD)} completed successfully: {table_name}")
            else:
                failed_tables.append(table_name)
                print(f"❌ Table {i}/{len(TABLES_TO_LOAD)} failed: {table_name}")
                
        except Exception as e:
            failed_tables.append(table_name)
            print(f"❌ Table {i}/{len(TABLES_TO_LOAD)} failed with error: {e}")
            print(f"Continuing with next table...")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"LOADING SUMMARY")
    print(f"{'='*80}")
    print(f"Total tables processed: {len(TABLES_TO_LOAD)}")
    print(f"Successful: {len(successful_tables)}")
    print(f"Failed: {len(failed_tables)}")
    
    if successful_tables:
        print(f"\n✅ Successfully loaded tables:")
        for table in successful_tables:
            print(f"   - {table}")
    
    if failed_tables:
        print(f"\n❌ Failed tables:")
        for table in failed_tables:
            print(f"   - {table}")
    
    return len(failed_tables) == 0

def update_version_tracking(source_table, org_id):
    """Update version tracking for the processed table"""
    try:
        # Check current version tracking
        version_df = spark.sql(f"""
            SELECT last_version FROM {org_version_table_name} 
            WHERE table_name = '{source_table}' AND org_id = '{org_id}'
        """)
        
        # Get current table version
        latest_version_df = spark.sql(f"DESCRIBE HISTORY {source_table} limit 1")
        current_version = str(latest_version_df.select("version").collect()[0][0])
        
        if version_df.count() == 0:
            # First time processing
            latest_version = spark.sql(f"DESCRIBE HISTORY {source_table} limit 1").select(F.max("version")).collect()[0][0]
            start_version = str(latest_version)
            print(f"📝 First time processing table for org {org_id}. Starting from version: {start_version}")
            spark.sql(f"""
                INSERT INTO {org_version_table_name} (table_name, org_id, last_version, updatedAt)
                VALUES ('{source_table}', '{org_id}', '{start_version}', current_timestamp())
            """)
        else:
            # Update existing version
            last_processed_version = version_df.collect()[0][0]
            
            if str(current_version) != str(last_processed_version):
                print(f"📝 Updating version from {last_processed_version} → {current_version}")
                spark.sql(f"""
                    UPDATE {org_version_table_name}
                    SET last_version = '{current_version}', updatedAt = current_timestamp()
                    WHERE table_name = '{source_table}' AND org_id = '{org_id}'
                """)
                print(f"✅ Version tracking updated.")
            else:
                print(f"ℹ️ No version change detected. Version tracking unchanged.")
                
    except Exception as e:
        print(f"⚠️ Warning: Could not update version tracking: {e}")

print("✅ Targeted table loading functions defined")


# COMMAND ----------

with open(table_info_path, 'r') as f:
    table_info_json = json.load(f)
    
# Create table config map
table_config_map = {table["tablename"]: table for table in table_info_json}

# COMMAND ----------

print(table_config_map)