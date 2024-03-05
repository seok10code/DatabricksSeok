# Databricks notebook source
#이 노트북은이 실습 랩 전체에서 노트북을 실행하기위한 Databricks 환경 설정을 처리합니다.
# 1. ADLS Gen2 파일 시스템을 초기화합니다.
# 2. Azure Data Lake Storage Gen2에 직접 연결하고 파일 시스템을 초기화합니다.

# 변수 선언. 다음 변수들은 전화 노트북으로 액세스 할 수 있습니다.
keyVaultScope = "key-vault-secrets"
adlsGen2AccountName = dbutils.secrets.get(keyVaultScope, "ADLS-Gen2-Account-Name")
fileSystemName = "transactions"
abfsUri = "abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/"

# ADLS Gen2 계정에 대한 OAuth 액세스에 필요한 구성 설정을 추가하십시오.
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(keyVaultScope, "Woodgrove-SP-Client-ID"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(keyVaultScope, "Woodgrove-SP-Client-Key"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/" + dbutils.secrets.get(keyVaultScope, "Azure-Tenant-ID") + "/oauth2/token")

# ADLS Gen2 계정에서 계층 네임 스페이스에 액세스하려면 파일 시스템을 초기화해야합니다. 이를 위해 마운트 작업 중에 파일 시스템을 만들 수 있도록`fs.azure.createRemoteFileSystemDuringInitialization` 구성 옵션을 사용합니다. ADLS Gen2 파일 시스템에 액세스하기 직전에이 값을 'true'로 설정 한 다음, 다음 명령어에 따라 'false'로 돌아갑니다.

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

# Azure Blob File System (ABFS) 구성표 (`abfss : // <file-system-name> @ <storage-account-name> .dfs.core.windows.net`)를 사용하여 ADLS Gen2 파일 시스템에 직접 액세스합니다.
dbutils.fs.ls(abfsUri)

# 초기화 중에 파일 시스템 생성을 비활성화합니다.
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

displayHTML("Direct connection to ADLS Gen2 account created and filesystem initialized...")
