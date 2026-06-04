# Databricks notebook source
!pip install msal openpyxl
dbutils.library.restartPython()

# COMMAND ----------

import msal

# COMMAND ----------

# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %run ./ReusableFunctions

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/RSDEuropeFinanceOperationalPricing/" library="Shared Documents" directory="/Automated Reports/Business Partner Creation Process - Daily Working Files EU/02-DE/" />
# client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
# sharepoint_scope_name='COMM_API_Cloud_Scope'
# sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
# tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
# drive_id = 'b!zs3sNP5AYkSw1qk2k5829z80auBHxhVIo51R7qLDe2EaKUBM7MwgQaHZOKKyWU5y'
# sharepoint_folder = '/Automated%20Reports/Business%20Partner%20Creation%20Process%20-%20Daily%20Working%20Files%20EU/02-DE'
# client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
# filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/collaboration/ProductDataAdmin" library="Disco" directory="/Disco SO" />
# client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
# sharepoint_scope_name='COMM_API_Cloud_Scope'
# sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
# tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
# drive_id = 'b!XTlDbA4CfE6kkA2FtFlgNG7Z8MG2wkhAqEDhYplax4vDz0zUMOw6QJeuo3VLD0M0'
# sharepoint_folder = '/Disco%20SO'
# client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
# filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/collaboration/ProductDataAdmin" library="Disco" directory="/Disco Xref" />
# client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
# sharepoint_scope_name='COMM_API_Cloud_Scope'
# sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
# tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
# drive_id = 'b!XTlDbA4CfE6kkA2FtFlgNG7Z8MG2wkhAqEDhYplax4vDz0zUMOw6QJeuo3VLD0M0'
# sharepoint_folder = '/Disco%20Xref'
# client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
# filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/collaboration/ProductDataAdmin/" library="Item creation reports" directory="/" />
# client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
# sharepoint_scope_name='COMM_API_Cloud_Scope'
# sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
# tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
# drive_id = 'b!XTlDbA4CfE6kkA2FtFlgNG7Z8MG2wkhAqEDhYplax4uh80UVUh_wRp4kYBmgzfOc'
# sharepoint_folder = '/'
# client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
# filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/collaboration/customerservicemanagers" library="Documents" directory="/T3 Daily Management/SO HOLD METRICS/Table for PowerBI" />
# client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
# sharepoint_scope_name='COMM_API_Cloud_Scope'
# sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
# tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
# drive_id = 'b!XTlDbA4CfE6kkA2FtFlgND2pu5UcmglPlyFyiCg39ARw7KnD4J8mQJMeVRq-T7ks'
# sharepoint_folder = '/T3 Daily Management/SO HOLD METRICS/Table for PowerBI'
# client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
# filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/SupplyChainFinanceRSDEU" library="/Shared Documents" directory="General/Actuals/2024/Demo" />
client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
sharepoint_scope_name='COMM_API_Cloud_Scope'
sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
drive_id = 'b!KQjjlTRCcUi_hkl83uA5Jw1gLdwctoBNvw12eXsMvS783tpc4KDMTZhWFxyunHXX'
sharepoint_folder = '/General/Actuals/2024/Demo'
client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
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

import msal

client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
sharepoint_scope_name='COMM_API_Cloud_Scope'
sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)

authority = f"https://login.microsoftonline.com/{tenant_id}"
scope = ["https://graph.microsoft.com/.default"]

app = msal.ConfidentialClientApplication(
    client_id, authority=authority, client_credential=client_secret
)
result = app.acquire_token_for_client(scopes=scope)

if "access_token" not in result:
    raise Exception(f"Token error: {result}")

access_token = result["access_token"]
print("✅ Graph access token acquired")


# COMMAND ----------

import requests
# https://thermofisher.sharepoint.com/sites/collaboration/customerservicemanagers/Documents//T3%20Daily%20Management/SO%20HOLD%20METRICS/Table%20for%20PowerBI/Holds_History_Line_daily.csv:
site_hostname = "thermofisher.sharepoint.com"
site_path = "sites/RSDEUSupplyChainManagement/"
drive_name="Distribution Operations"
# Get site info (includes siteId)
site_info_url = f"https://graph.microsoft.com/v1.0/sites/{site_hostname}:/{site_path}"
headers = {"Authorization": f"Bearer {access_token}"}

site_info = requests.get(site_info_url, headers=headers).json()
site_id = site_info["id"]

# Get all document libraries (drives) in this site
drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
drives = requests.get(drives_url, headers=headers).json()

# Find provided drive_name
drive_id = None
for d in drives["value"]:
    print(f"Drive: {d['name']}")
    if d["name"] == drive_name:
        drive_id = d["id"]
        break

if not drive_id:
    raise Exception(f"Drive '{drive_name}' not found")

print(f"✅ {drive_name} driveId:", drive_id)


# COMMAND ----------

drive_id="b!w3mViijZBEq3rmFrYrabSDkBftQtWS5LjxypItzPZ3ooG31Eh5OERpO_ND5ZoEkT"
sharepoint_folder="/Compliance%20-%20Operations%20Support/02_Germany/Daily%20Stock_CLP%20Record"
list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{sharepoint_folder}:/children"
items = requests.get(list_url, headers=headers).json()

print("📂 Items in folder:")
print(len(items.get("value", [])))
for item in items.get("value", []):
    print(f"- {item['name']} ({'Folder' if 'folder' in item else 'File'})")

# COMMAND ----------

# ==== 4. Locate file inside folder ====
file_name = "Daily ICPE stock_20250919_0506.xlsx"   # <-- change to your filename

file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{sharepoint_folder}/{file_name}"

file_info = requests.get(file_url, headers=headers).json()

if "@microsoft.graph.downloadUrl" not in file_info:
    raise Exception("Download URL not found for file")

download_url = file_info["@microsoft.graph.downloadUrl"]

# COMMAND ----------

resp = requests.get(download_url)
local_path="/Volumes/ccg_raw_test/config/control_entries_volume/report_watcher_output/sample_outputs/"+file_name
if resp.status_code == 200:
    with open(local_path, "wb") as f:
        f.write(resp.content)
    print(f"✅ File downloaded → {file_name}")
else:
    print("❌ Error downloading file:", resp.status_code, resp.text)

# COMMAND ----------

 upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{sharepoint_folder}/{file_name}:/content"
response = requests.put(upload_url, headers=headers, data=file_path)

# COMMAND ----------

def upload_file_to_sharepoint(client_id, sharepoint_scope_name, sharepoint_key, tenant_id, drive_id, sharepoint_folder, file_name, file_path):
    try:
        # Retrieve the client secret from secrets storage
        client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)

        # Authenticate with Microsoft Graph
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
        scopes = ["https://graph.microsoft.com/.default"]

        # Get access token
        token_response = app.acquire_token_for_client(scopes=scopes)

        if 'access_token' not in token_response:
            print("Failed to acquire token.")
            raise Exception("Access token acquisition failed.")

        headers = {
            'Authorization': 'Bearer ' + token_response['access_token'],
            'Content-Type': 'application/json'
        }

        upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{sharepoint_folder}/{file_name}:/content"
        response = requests.put(upload_url, headers=headers, data=file_path)

        if response.status_code in (201, 200):
            print(f"{datetime.now()}: file: {file_name} uploaded to Sharepoint:{sharepoint_folder}/{filename}")
        else:
            print(f"Error uploading file: {response.status_code} - {response.text}")
            raise Exception(f"{datetime.now()}:Error uploading file: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"{datetime.now()}:An error occurred: {str(e)}")
        raise e

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/RSDEuropeFinanceAPTreasury/AP%20Europe Supervisor%20team" />
client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
sharepoint_scope_name='COMM_API_Cloud_Scope'
sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
drive_id = 'b!eUokb3qjekir0GVV5GDt6L4uldSv5y1NnIc-MMRKGTQyyPxMWeKDTpTnCjt0-EwP'
sharepoint_folder = '/DPO'
client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
filename="Test.xlsx"

# COMMAND ----------

# <sharepoint url="https://thermofisher.sharepoint.com/sites/RSDEuropeFinanceAPTreasury/AP%20Europe Supervisor%20team" />
client_id="6a2ffa46-eb5c-4e23-8a54-ffe5298f14b8"
sharepoint_scope_name='COMM_API_Cloud_Scope'
sharepoint_key='Sharepoint_CCG_RSD_EU_DATA_API_Token'
tenant_id='b67d722d-aa8a-4777-a169-ebeb7a6a3b67'
drive_id="b!w3mViijZBEq3rmFrYrabSDkBftQtWS5LjxypItzPZ3ooG31Eh5OERpO_ND5ZoEkT"
sharepoint_folder="/Compliance%20-%20Operations%20Support/09_Spain/Daily%20Stock_CLP%20Record"
client_secret = dbutils.secrets.get(sharepoint_scope_name, sharepoint_key)
filename="Test.xlsx"

# COMMAND ----------

dummy_df=clean_string_column(dummy_df)

# COMMAND ----------

# Upload the file to Sharepoint and send the mail confirmation
dataframes = {
    "Sheet1": dummy_df
}

# Create email message

try:
    with io.BytesIO() as xlsx_buffer:
        with pd.ExcelWriter(xlsx_buffer, engine='xlsxwriter') as writer:
            for sheet_name, pandas_df in dataframes.items():
                pandas_df = pandas_df.toPandas() if hasattr(pandas_df, 'toPandas') else pandas_df
                if pandas_df.empty:
                    pandas_df.to_excel(writer, index=False, sheet_name=sheet_name, header=False)
                else:
                    pandas_df.to_excel(writer, index=False, sheet_name=sheet_name, header=True)
                
                    # Get the xlsxwriter workbook and worksheet objects
                    workbook  = writer.book
                    worksheet = writer.sheets[sheet_name]

                    plain_format = workbook.add_format({"bold": False, "border": 0})

                    # Rewrite header row (row=0) with plain format
                    for col_num, value in enumerate(pandas_df.columns.values):
                        worksheet.write(0, col_num, value, plain_format)


        xlsx_buffer.seek(0)
        
        upload_file_to_sharepoint(client_id, sharepoint_scope_name, sharepoint_key, tenant_id, drive_id, sharepoint_folder, filename, xlsx_buffer.getvalue())
        body=f"{body} : {sharepoint_url}/{sharepoint_folder}/{filename}"

        # Send the email with retry logic
        msg.attach(MIMEText(body, 'plain'))
        send_email_with_retry(smtp_server, smtp_port, msg, sender_email, receiver_email.split(','))

except Exception as e:
    print(f"{datetime.now()} : Error occurred while processing {filename}: {e}")
    # msg.attach(MIMEText(f"Error occurred while processing {filename}:\n{e}", 'plain'))
    # send_email_with_retry(smtp_server, smtp_port, msg, sender_email, sender_email.split(','))
    raise e