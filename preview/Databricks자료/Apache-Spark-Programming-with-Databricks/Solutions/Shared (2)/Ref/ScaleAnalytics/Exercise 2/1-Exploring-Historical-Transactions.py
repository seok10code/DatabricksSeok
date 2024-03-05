# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2 - Task 7: Exploring historical transaction data with Azure Databricks and Spark
# MAGIC
# MAGIC 데이터 준비는 데이터 엔지니어링 프로세스에서 중요한 단계입니다. 데이터를 사용하여 기계 학습 모델을 만들기 전에 데이터를 이해하고 의도 한 목적에 유용한 데이터 세트의 기능을 결정하는 것이 중요합니다. 이를 지원하기 위해 Woodgrove는 탐색을위한 일부 기록 트랜잭션이 포함 된 CSV 파일을 제공했습니다.
# MAGIC
# MAGIC 이 노트북에서는 Woodgrove가 제공 한이 원시 트랜잭션 데이터를 다운로드하여 탐색하여 사기를 감지하는 기계 학습 모델 구축 및 교육에 사용하기 위해 데이터에 대해 수행해야하는 변환 유형을 더 잘 이해할 수 있습니다. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach notebook to you cluster
# MAGIC
# MAGIC 노트북에서 셀을 실행하기 전에 해당 셀을 클러스터에 연결해야합니다. 노트북의 도구 모음에서 드롭 다운 화살표를 선택한 다음 연결 대상에서 클러스터를 선택하십시오.
# MAGIC
# MAGIC ![Detach is expanded in the notebook toolbar, and the cluster is highlighted under Attach to.](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-attach-notebook.png "Attach notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qurey historical transaction data from DW
# MAGIC
# MAGIC Day1에서 Transaction Data를 Cloud DW로 마이그레이션 하였습니다.
# MAGIC 아래 셀을 실행하여 DW에서 바로 Transaction Data를 조회할 수 있습니다. 테이블 이름은 `dbo.CardTransaction` 입니다
# MAGIC
# MAGIC 처음 JDBC연결에 필요한 필수정보를 아래 셀과같이 구성하여 줍니다.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val jdbcHostname = "<HOSTNAME e.g. cohodwserver.database.windows.net>"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "<DBNAME e.g. CohoDB>"
# MAGIC
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC
# MAGIC val jdbcUsername = "<USER_NAME>"
# MAGIC val jdbcPassword = "<PASSWORD>"
# MAGIC
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")

# COMMAND ----------

# MAGIC %md
# MAGIC 필수 연결정보를 넣었다면, 접속이 잘 되는지 다음셀을 통해 Test를 합니다.

# COMMAND ----------

# MAGIC %scala 
# MAGIC import java.sql.DriverManager
# MAGIC val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
# MAGIC connection.isClosed()

# COMMAND ----------

# MAGIC %md
# MAGIC Card Teamsation의 Table 이름은 `dbo.CardTransaction` 입니다. 해당 테이블의 Schema를 확인하여 봅니다.

# COMMAND ----------

# MAGIC %scala
# MAGIC val CardTransaction_table = spark.read.jdbc(jdbcUrl, "dbo.CardTransaction", connectionProperties)
# MAGIC CardTransaction_table.printSchema

# COMMAND ----------

# MAGIC %md
# MAGIC `dbo.CardTransaction`에서 1000건만 가져와서 데이터를 확인하여 봅니다.

# COMMAND ----------

# MAGIC %scala 
# MAGIC
# MAGIC val custom_query = "(select top(1000) * from dbo.CardTransaction) ct_alias"
# MAGIC val df = spark.read.jdbc(url=jdbcUrl, table=custom_query, properties=connectionProperties)
# MAGIC display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download historical transaction data
# MAGIC
# MAGIC Woodgrove는 또한 트랜잭션 Raw 데이터를 Azure Blob Storage 계정에 배치했으며 해당 컨테이너에 대한 액세스 권한을 부여했습니다. 아래의 셀을 실행하여`wget`을 사용하여 해당 컨테이너에서 트랜잭션 CSV 파일을 다운로드하고 DBFS (Databricks File System) 위치에 저장합니다.

# COMMAND ----------

import uuid
import os

# Create a temporary folder to store locally relevant content for this notebook
tempFolderName = '/FileStore/mcw_cdb_{0}'.format(uuid.uuid4())
dbutils.fs.mkdirs(tempFolderName)
print('Content files will be saved to {0}'.format(tempFolderName))

fileToDownload = 'Untagged_Transactions.csv'

downloadCommand = 'wget -O ''/dbfs{0}/{1}'' ''https://databricksdemostore.blob.core.windows.net/data/mcw-cdb/{1}'''.format(tempFolderName, fileToDownload)
os.system(downloadCommand)
  
#List all downloaded files
dbutils.fs.ls(tempFolderName)

# COMMAND ----------

# MAGIC %md
# MAGIC 아래의 셀을 실행하여 다운로드 CSV 파일을 Spark DataFrame으로 읽으면 데이터를 쉽게 탐색 할 수 있습니다. Spark가 파일의 첫 번째 행을 열 헤더로 읽도록하고 내용에 따라 각 열의 스키마를 유추하도록 지시하는`options` 메소드 사용에 주목하시기 바랍니다.

# COMMAND ----------

transactions = spark.read.format('csv').options(header='true', inferSchema='true').load(tempFolderName + '/' + fileToDownload)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the transaction data
# MAGIC
# MAGIC Woodgrove의 트랜잭션 데이터를 사용하여 사기를 탐지하는 사용자 지정 기계 학습 모델을 구축하므로 작업 할 데이터를 더 잘 이해하는 것이 중요합니다.
# MAGIC
# MAGIC `display` 방법을 사용하면 다운로드한 CSV 파일에 포함된 트랜잭션 데이터를 검사 할 수 있습니다.
# MAGIC
# MAGIC `display ()`명령의 출력으로 열 이름과 각 열에 저장된 값을 포함하여 데이터 세트에 포함 된 데이터 열을 검사 할 수 있습니다. 이 정보를 사용하면 트랜잭션 데이터에 대한 탐색 적 분석을 시작할 수 있으며, 트랜잭션이 사기 가능성이 있는지 여부를 판별하는 데 유용한 정보와 빈 값과 널값을 포함하는 열을 포함하는 필드를 찾을 수 있습니다. 이는 사기 분석에 유용한 열과 데이터 집합에서 제거 할 수있는 열을 결정하는 데 도움이됩니다.
# MAGIC
# MAGIC 아래 셀을 실행하고 Woodgrove의 트랜잭션 로그에 수집되는 정보 유형을 더 잘 이해하기 위해 몇 가지 데이터를 살펴보십시오.

# COMMAND ----------

display(transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review columns with empty or `null` values
# MAGIC
# MAGIC 우선 비어 있거나 'null'행이있는 몇 개의 열을 검사하여 고유 한 값의 목록을 얻습니다. 비어 있거나 널 (null) 값만 포함하는 열은 머신 러닝 모델을 구축하고 교육하는 데 사용되는 데이터 집합에서 제외시킬 수 있습니다. 빈 값을 포함하는 DataFrame의 다른 열에 대해 아래 셀을 다시 실행하여 빈 값만 포함하는지 또는 일부 레코드에 데이터가 포함되어 있는지 확인하십시오.

# COMMAND ----------

transactions.select("browserType").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect column values
# MAGIC
# MAGIC 비어있는 필드가 검사된 후에, 우리의 사기 탐지 시나리오에 대해 유용한 정보를 제공하지 않는 값과 열이 있는지 확인합니다. 예를 들어, 아래 명령을 사용하여`transactionScenario` 및`transactionType` 필드에 포함 된 고유(Distinct) 값을 찾아봅니다.

# COMMAND ----------

transactions.select("transactionScenario").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 위의 결과에서 볼 수 있듯이, transactionScenario 및 transactionType 필드에는 각각 하나의 값만 갖고있어 거래의 사기 여부 판단에 거의 가치가 없습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look for invalid values
# MAGIC
# MAGIC 계속 진행하기 전에 데이터 탐색을 수행 할 때 고려해야 할 또 하나의 시나리오를 살펴 보겠습니다.
# MAGIC
# MAGIC Woodgrove는 가능한 'cvvVerifyResult'값과 각각의 의미 목록을 제공했습니다.
# MAGIC
# MAGIC | Code  | Meaning
# MAGIC | ----- | ---------
# MAGIC | Empty | Transaction failed because wrong CVV2 number was entered or no CVV2 number was entered
# MAGIC | M     | CVV2 Match
# MAGIC | N     | CVV2 No Match
# MAGIC | P     | Not Processed
# MAGIC | S     | Issuer indicates that CVV2 data should be present on the card, but the merchant has indicated data is not present on the card
# MAGIC | U     | Issuer has not certified for CVV2 or Issuer has not provided Visa with the CVV2 encryption keys
# MAGIC
# MAGIC 아래의 셀을 실행하여`cvvVerifyResult` 필드에 포함 된 고유 값과 각 수를보십시오. 이 경우에는 groupBy () 메서드를 사용하여 각각의 개수와 함께 고유 한 'cvvVerifyResult'값을 제공합니다.
# MAGIC
# MAGIC > 여기에 사용 된 데이터의 스키마 세부 사항은 <https://microsoft.github.io/r-server-fraud-detection/input_data.html>에서 확인할 수 있습니다.

# COMMAND ----------

transactions.groupBy("cvvVerifyResult").count().sort("cvvVerifyResult").show()

# COMMAND ----------

# MAGIC %md
# MAGIC `cvvVerifyResult` 필드의 값에 주목해야 할 두 가지 흥미로운 점이 있습니다.
# MAGIC
# MAGIC   1. Woodgrove에서 제공한 목록에 따르면 빈 값을 가진 일부 행이 있는데, 이는 실패한 트랜잭션을 나타냅니다. 사기 탐지 모델의 경우에는 완료된 트랜잭션에만 관심이 있으므로`cvvVerifyResult`가 비어있는 행을 삭제하는 것을 생각해 볼 수 있습니다.
# MAGIC   2. 목록에 따라 적은 양이지만 유효하지 않은 값이 (예 : X 또는 Y)이 있습니다. 모델에서 어떻게 처리할 지 생각해 볼 필요가 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review column data types
# MAGIC
# MAGIC 데이터의 또 다른 관점으로 데이터의 유형(Type)을 볼 수 있습니다. 각 열에 할당 된 데이터 형식을 보려면 DataFrame에서`printSchema ()`메서드를 사용할 수 있습니다.

# COMMAND ----------

transactions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 모델을 작성할 때 각 필드가 열에 저장된 데이터 유형을 반영하고 일부 열을보다 적절한 유형으로 캐스트하는 것을 고려하십시오. 예를 들어 'transactionIPaddress'필드는 현재 'double'로 표시되지만 사용자 IP 주소의 마지막 두 옥텟을 포함하므로 'string'값으로 더 잘 표시 될 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC 거래 데이터 세트 내에서 정보의 종류를 조사한 후 사기 탐지 기계 학습 모델을 구축 할 때 어떤 데이터 준비 단계를 수행해야하는지 더 잘 이해해야합니다. Exercise 3에서 데이터 준비를 수행하는 방법에 대해 자세히 알아 봅니다.
# MAGIC
# MAGIC 지금은 Cosmos DB에서 스트리밍 트랜잭션을 가져 오는 작업으로 넘어 갑니다.
# MAGIC
# MAGIC
# MAGIC 이제 [Exercise 2 - Task 7: Responding to streaming transactions using the Cosmos DB Change Feed and Spark Structured Streaming in Azure Databricks]($./2-Cosmos-DB-Change-Feed) 노트북으로 이동해야합니다. 여기에서는 Spark Structured Streaming을 사용하여 Cosmos DB 변경 피드에서 거의 실시간으로 데이터를 수집하고 가져온 데이터를 Azure Databricks Delta 테이블에 쓰는 방법을 검토합니다.
