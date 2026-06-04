# Databricks notebook source
# DBTITLE 1,Install library
!pip install smbprotocol
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Establish connection
import smbclient
import io
 
# 2) Configuration
# server   = "SEVAS-FS01.emea.thermo.com" 
# server= "sevas-fms01.emea.thermo.com"
# server = "uklbh-fs-p4.emea.thermo.com"
server = "pai01mpf01.ads.invitrogen.net"
# server ="begelnetapcnas1.thermo.com"
# server="awse1-0ffsxf018.thermo.com"
# # share    = r"Public\Statistik\Intrastat_SE"  
# share    = r"Public\Statistik\Thermo Scientific Genomics" 
# share = r"Data\GCLBH\IBS_extract"
share = r"AS400_Fusion\Integrator\Extracts\Business_Partner_Creation" 
# share = r"AS400_Fusion\Integrator\Extracts\Item_Creation_Webable"
# share=r"AS400_Fusion\Integrator\Extracts\Item_Creation"
# share = r"Public\Customer Service & Product Support\05 Statistics\Order Intake Weekly Nordics"
# share = r"Public\Customer Service & Product Support\05 Statistics\Order Intake Nordics"       
# share = r"ftpas400\IBS_UK"
# share = r"Data\GCLBH\IBS_extract"        
# share = r"AllItems"
# share = r"Public\Statistik\Intrastat_SE"
# share=r"Public"
username = "uklbh.svc.erp"
# password = "Fu9eBu+2{88}Svc"
password=dbutils.secrets.get("COMM_API_Cloud_Scope", "samba_uklbh_svc_erp_PassWord")

 
# 3) Register a session
smbclient.register_session(
    server,
    username=username,
    password=password
)
 

# COMMAND ----------

# DBTITLE 1,List Files on Share
path = rf"\\{server}\{share}"

# List files
for file in smbclient.listdir(path):
    if file.startswith("Test_EDP"):
        print(file)

# for file in smbclient.listdir(path):
#     print(file)

# COMMAND ----------

from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect

server = "SEVAS-FS01.emea.thermo.com"
username = "uklbh.svc.erp"
password=dbutils.secrets.get("COMM_API_Cloud_Scope", "samba_uklbh_svc_erp_PassWord")
share = "Public"   # <-- must be the actual share name

# 1. Create a connection (TCP to port 445)
conn = Connection(server=server, port=445)
conn.connect()

# 2. Authenticate a session
session = Session(conn, username, password)
session.connect()

# 3. Connect to the share (only share name here, not full path)
tree = TreeConnect(session, fr"\\{server}\{share}")
tree.connect()



# COMMAND ----------

# DBTITLE 1,Get Modification time
import datetime
remote_path = rf"\\{server}\{share}\EDP_Holding_Extract_02_20250925.xlsx"
st = smbclient.stat(remote_path)

# Convert to human-readable datetime
mod_time = datetime.datetime.fromtimestamp(st.st_mtime, datetime.timezone.utc)

print(f"File: {remote_path}")
print(f"Size: {st.st_size} bytes")
print(f"Last modified: {mod_time}")

# COMMAND ----------

# DBTITLE 1,Download from SMB
local_file = r"/Volumes/ccg_raw_test/config/control_entries_volume/report_watcher_output/sample_outputs/Holding_Extract_02_20250924.xlsx"
remote_file = rf"{path}\Holding_Extract_02_20250924.xlsx"
# Open remote file and write to local file
with smbclient.open_file(remote_file, mode="rb") as src, open(local_file, "wb") as dst:
    dst.write(src.read())
print(f"File Downloaded: {local_file}")

# COMMAND ----------

# DBTITLE 1,Delete File from Samba Server
remote_path = rf"\\{server}\{share}\Test_EDP_Holding_Extract_14_20250925.xlsx"
smbclient.remove(remote_path)

print(f"Deleted file: {remote_path}")

# COMMAND ----------

# DBTITLE 1,Download in chunks
with smbclient.open_file(remote_file, mode="rb") as src, open(local_file, "wb") as dst:
    for chunk in iter(lambda: src.read(4096), b""):
        dst.write(chunk)

# COMMAND ----------



# COMMAND ----------

remote_path = f"\\\\{samba_server}\\{samba_share}\\{remote_file_name}"
print(f"remote path: {remote_path}")
with smbclient.open_file(remote_path, mode="wb") as remote_file:
  remote_file.write(local_file_path)