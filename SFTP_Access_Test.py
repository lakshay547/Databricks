# Databricks notebook source
# MAGIC %pip install paramiko
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

!pip install pysftp==0.2.9 paramiko==2.11.0
dbutils.library.restartPython()

# COMMAND ----------

!pip install pysftp==0.2.9 paramiko==2.11.0
dbutils.library.restartPython()

# COMMAND ----------

import pysftp

# COMMAND ----------

import paramiko

# COMMAND ----------

import socket
hostname = 'ftp3.dhl.com'

username = 'lch9f7yj'
port=22
password = 'Seo47d$Sd#u;AXgC'

# Connect to get the server host key
sock = socket.create_connection((hostname, port))
transport = paramiko.transport.Transport(sock)
transport.start_client()

# Get the host key
key = transport.get_remote_server_key()

# Save to known_hosts format
print(f"{hostname} {key.get_name()} {key.get_base64()}")

transport.close()


# COMMAND ----------

# MAGIC %run ../../DA_CCG_RSD_EU/Constants

# COMMAND ----------

# MAGIC %run ../../DA_CCG_RSD_EU/ReusableFunctions

# COMMAND ----------

filename="Test.xlsx"

# COMMAND ----------

data = [
    ("Alice", 1, "New York"),
    ("Bob", 2, "London"),
    ("Charlie", 3, "Paris")
]

columns = ["Name", "ID", "City"]

dummy_df = spark.createDataFrame(data, columns)

display(dummy_df)

# COMMAND ----------

# DBTITLE 1,Use Private Key
import paramiko

hostname = 'rsd.cforia.com'
username = 'RSD_sftp_user'
private_key_path = '/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/A06987_Cforia_TMO_RSD_private.key'  # or .ppk for PuTTY keys
private_key_pass = 'Cforia-Thermo2-public.key.pgp'       # or None if unencrypted
remote_dir = f'/CCG-IBS-Prod'

key = paramiko.RSAKey.from_private_key_file(private_key_path, password=private_key_pass)

# Establish transport and connection
transport = paramiko.Transport((hostname, 22))
transport.connect(username=username, pkey=key)

sftp = paramiko.SFTPClient.from_transport(transport)

# Example: list files in remote directory
print("Files:", sftp.listdir(remote_dir))


# COMMAND ----------

import paramiko
filename="Test.txt"
# SFTP connection details
hostname = 'rsd.cforia.com'
username = 'RSD_sftp_user'
private_key_path = '/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/A06987_Cforia_TMO_RSD_private.key'
private_key_pass=None    
remote_dir = f'/CCG-IBS-Test/'

key = paramiko.RSAKey.from_private_key_file(private_key_path, password=private_key_pass)

# File paths
remote_file_path = f'{remote_dir}{filename}.pgp'
local_file_path = f"/Volumes/ccg_raw/config/control_entries_volume/report_watcher_output/{filename}.pgp"

# 1. Create transport
transport = paramiko.Transport((hostname, 22))
transport.connect(username=username, pkey=key)

# 2. Create SFTP client
sftp = paramiko.SFTPClient.from_transport(transport)

# 3. Upload the file
sftp.put(local_file_path, remote_file_path)

print(f"File uploaded to {hostname}:{remote_file_path}")

# 4. Close connection
sftp.close()
transport.close()


# COMMAND ----------

# DBTITLE 1,Using Paramiko

# SFTP connection parameters
hostname = 'ftp3.dhl.com'
username = 'lch9f7yj'
port=22
password = 'Seo47d$Sd#u;AXgC'
remote_path = f'/in/GB/work/{filename}'

# Connect and upload
try:
    pandas_df = dummy_df.toPandas()

    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
      pandas_df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Create SSH client
    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(hostname=hostname, port=port, username=username, password=password)

    # Create SFTP client
    sftp = ssh.open_sftp()

    # Upload file
    sftp.put(xlsx_buffer.read(), remote_file_path)

    print(f"✅ File uploaded to {remote_file_path} on {hostname}")

    # Close connection
    sftp.close()
    transport.close()

except Exception as e:
    print(f"❌ Failed to upload file: {e}")


# COMMAND ----------

pandas_df = dummy_df.toPandas()

xlsx_buffer = io.BytesIO()
with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
    pandas_df.to_excel(writer, index=False, sheet_name='Sheet1')

# Go back to start of buffer before upload
xlsx_buffer.seek(0)

# --- Step 3: Connect to SFTP and upload
hostname = 'ftp3.dhl.com'
username = 'lch9f7yj'
scope_name="COMM_API_Cloud_Scope"
key="FTP_DHL_PassWord"
password=dbutils.secrets.get(scope_name,key)
# password = 'Seo47d$Sd#u;AXgC'
remote_path = f'/in/GB/work/{filename}'
print(remote_path)

# Optional: Disable host key checking (for dev/testing only)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    with sftp.open(remote_path, 'wb') as remote_file:
        remote_file.write(xlsx_buffer.read())  # Upload the in-memory file content

print(f"Uploaded Excel file to {remote_path} on SFTP")

# --- Step 4: Cleanup
xlsx_buffer.close()

# COMMAND ----------

# DBTITLE 1,Upload File to SFTP server
pandas_df = dummy_df.toPandas()

xlsx_buffer = io.BytesIO()
with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
    pandas_df.to_excel(writer, index=False, sheet_name='Sheet1')

# Go back to start of buffer before upload
xlsx_buffer.seek(0)

# --- Step 3: Connect to SFTP and upload
hostname = 'ftp3.dhl.com'
username = 'lch9f7yj'
password = 'Seo47d$Sd#u;AXgC'
remote_path = f'/in/GB/work/{filename}'
print(remote_path)

# Optional: Disable host key checking (for dev/testing only)
# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None

with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    with sftp.open(remote_path, 'wb') as remote_file:
        remote_file.write(xlsx_buffer.read())  # Upload the in-memory file content

print(f"Uploaded Excel file to {remote_path} on SFTP")

# --- Step 4: Cleanup
xlsx_buffer.close()

# COMMAND ----------

# DBTITLE 1,List all files in remote directory at SFTP server
hostname = 'ftp3.dhl.com'
port = 22
username = 'lch9f7yj'
password = 'Seo47d$Sd#u;AXgC'

# cnopts = pysftp.CnOpts()
# cnopts.hostkeys = None

with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    remote_dir = f'/in/GB/work/'
    file_list = sftp.listdir(remote_dir)

    print(f"Files in {remote_dir}:")
    for file in file_list:
        print(file)


# COMMAND ----------

# DBTITLE 1,Delete File from SFTP server
hostname = 'ftp3.dhl.com'
port = 22
username = 'lch9f7yj'
password = 'Seo47d$Sd#u;AXgC'

# Disable host key checking (testing only)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    remote_file = f'/in/GB/work/{filename}'
    sftp.remove(remote_file)
    print(f"Deleted: {remote_file}")


# COMMAND ----------

# DBTITLE 1,Download File from SFTP server
hostname = 'ftp3.dhl.com'
port = 22
username = 'lch9f7yj'
password = 'Seo47d$Sd#u;AXgC'

remote_file_path = f'/in/GB/work/{filename}'
import os
local_file_path = f"/FileStore/SFTP_file/{filename}"
os.makedirs(os.path.dirname(local_file_path), exist_ok=True)


# Optional: Disable host key verification (not for production)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
    sftp.get(remote_file_path, local_file_path)  # Download file
    print(f"Downloaded {remote_file_path} to {local_file_path}")
