# Databricks notebook source
pip install pgpy

# COMMAND ----------

pip install python-gnupg

# COMMAND ----------

import gnupg

# COMMAND ----------

public_key_path='/Volumes/ccg_raw_test/config/control_entries_volume/report_watcher_output/Keys/Cforia-Thermo2-public.key.pgp'
gpg = gnupg.GPG(gnupghome=None)
with open(public_key_path, 'r') as f:
  key_data = f.read()
import_result = gpg.import_keys(key_data)
print(import_result)
keyid = gpg.list_keys()[0]['keyid']
print(keyid)

# COMMAND ----------

import gnupg

def encrypt_file_with_gnupg(input_file_path, public_key_path, output_file_path, recipient_email_or_keyid, gpg_home=None):
    gpg = gnupg.GPG(gnupghome=gpg_home)

    # Import the public key
    with open(public_key_path, 'r') as f:
        key_data = f.read()
    import_result = gpg.import_keys(key_data)

    if not import_result.count:
        raise ValueError("❌ Failed to import the public key.")

    # Encrypt the file
    with open(input_file_path, 'rb') as f:
        status = gpg.encrypt_file(
            f,
            recipients=[recipient_email_or_keyid],  # ✅ required!
            output=output_file_path,
            always_trust=True
        )

    if status.ok:
        print(f"✅ File encrypted successfully: {output_file_path}")
    else:
        raise Exception(f"❌ Encryption failed: {status.status}")


# COMMAND ----------

encrypt_file_with_gnupg(
    input_file_path='/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/pipe_2.txt',
    public_key_path='/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/Cforia-Thermo2-public.key.pgp',
    output_file_path='/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/pipe_2.txt.pgp',
    recipient_email_or_keyid='DEB0E2D3A1386B98'
)

# COMMAND ----------

from pgpy import PGPKey, PGPUID
from pgpy.constants import PubKeyAlgorithm, KeyFlags, HashAlgorithm, SymmetricKeyAlgorithm, CompressionAlgorithm

# Generate new key
key = PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 2048)
uid = PGPUID.new('Test User', email='test@example.com')

key.add_uid(uid,
            usage={KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
            hashes=[HashAlgorithm.SHA256],
            ciphers=[SymmetricKeyAlgorithm.AES256],
            compression=[CompressionAlgorithm.ZLIB])

# Save keys
with open("private_key.asc", "w") as f:
    f.write(str(key))

with open("public_key.asc", "w") as f:
    f.write(str(key.pubkey))


# COMMAND ----------

import pgpy

def encrypt_file_with_pgp(input_file_path, public_key_path, output_file_path):
    # Load the public key
    with open(public_key_path, 'r') as key_file:
        key_data = key_file.read()

    public_key, _ = pgpy.PGPKey.from_blob(key_data)

    # Read the file to encrypt
    with open(input_file_path, 'rb') as f:
        file_bytes = f.read()

    # Create a PGP message from the file content
    message = pgpy.PGPMessage.new(file_bytes, file=True)

    # Encrypt the message with the public key
    encrypted_message = public_key.encrypt(message)

    # Save the encrypted message to a .pgp file
    with open(output_file_path, 'w') as f:
        f.write(str(encrypted_message))

    print(f"✅ File encrypted and saved as: {output_file_path}")


# COMMAND ----------

input_file_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/F_IBS_ARCUST_20250807030005.txt"
public_key_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/Cforia-Thermo2-public.key.pgp"
output_file_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/"
encrypt_file_with_pgp(input_file_path, public_key_path, output_file_path)

# COMMAND ----------

import pgpy

def decrypt_pgp_file(encrypted_file_path, private_key_path, output_file_path):
    # Load the private key
    with open(private_key_path, 'r') as key_file:
        key_data = key_file.read()

    private_key, _ = pgpy.PGPKey.from_blob(key_data)

    # Unlock the private key using passphrase
    # if private_key.is_protected:
    #     private_key.unlock(passphrase)

    # Load the encrypted file
    encrypted_message = pgpy.PGPMessage.from_file(encrypted_file_path)

    # Decrypt the message
    decrypted_message = private_key.decrypt(encrypted_message).message

    # Write the decrypted content to a file
    with open(output_file_path, 'w') as out_file:
        out_file.write(decrypted_message)

    print(f"✅ Decrypted output saved to: {output_file_path}")


# COMMAND ----------

encrypted_file_path = "/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/F_IBS_ARCUST_20180921102457.txt.pgp"
# private_key_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/Cforia-Thermo2-public.key.pgp"
private_key_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/A06987_Cforia_TMO_RSD_private.key"
output_file_path="/Workspace/DA_CCG_DEV/DA_CCG_RSD_EU_DQM_CL/DAILY/"
decrypt_pgp_file(encrypted_file_path, private_key_path, output_file_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ccg_raw_test.rsd_eu_ibs.Z2OCFICUS where SOURCE_COUNTRY='SYS' AND CURRENT=TRUE