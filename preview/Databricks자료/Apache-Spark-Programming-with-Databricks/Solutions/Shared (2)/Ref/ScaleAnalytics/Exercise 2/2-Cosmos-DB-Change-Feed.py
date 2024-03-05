# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 2 - Task 8: Responding to streaming transactions using the Cosmos DB Change Feed and Spark Structured Streaming in Azure Databricks
# MAGIC
# MAGIC 이 노트북에서는 [Azure Cosmos DB Spark Connector](https://github.com/Azure/azure-cosmosdb-spark)를 사용하여 [Azure Cosmos DB Change Feed](https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed)에 연결합니다. Cosmos DB 변경 피드 및 [Spark Structured Streaming](https://docs.azuredatabricks.net/spark/latest/structured-streaming/index.html)을 사용하여 Cosmos DB에서 수집 된 데이터를`transactions` Databricks Delta table에 기록하며 Databricks Delta 테이블은 거의 실시간으로 스트리밍됩니다.
# MAGIC
# MAGIC 클러스터는 이미 Azure Cosmos DB Spark 커넥터로 구성되었습니다. 참고로 다음 Maven 패키지를 추가하여 설치할 수 있습니다.
# MAGIC
# MAGIC **azure-cosmosdb-spark**
# MAGIC * Source: Maven Coordinate
# MAGIC * 검색 스파크 패키지 및 Maven Central을 선택하십시오.
# MAGIC * 패키지 검색 패키지 대화 상자에서 드롭 다운을 Maven Central로 변경 한 다음`azure-cosmosdb-spark '를 검색하고 아티팩트 ID가'azure-cosmosdb-spark_2.4.0_2.11 '인지 확인하고 릴리스가'1.4.1 '인지 확인하십시오. `.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach notebook to you cluster
# MAGIC
# MAGIC 노트북에서 셀을 실행하기 전에 해당 셀을 클러스터에 연결해야합니다. 노트북의 도구 모음에서 드롭 다운 화살표를 선택한 다음 연결 대상에서 클러스터를 선택하십시오.
# MAGIC
# MAGIC ![Detach is expanded in the notebook toolbar, and the cluster is highlighted under Attach to.](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-attach-notebook.png "Attach notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment setup
# MAGIC
# MAGIC ADLS Gen2 (Azure Data Lake Storage Gen2)의 기본 부분은 [계층 네임 스페이스] (https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace)를 추가하는 것입니다. )를 Blob 저장소에 추가 계층 적 네임 스페이스는 효율적인 데이터 액세스를 위해 객체 / 파일을 디렉토리 계층으로 구성합니다. 공통 오브젝트 저장소 이름 지정 규칙은 이름에 슬래시를 사용하여 계층 적 디렉토리 구조를 모방합니다. 이 구조는 ADLS Gen2에서 실현되었습니다. 디렉토리 이름 바꾸기 또는 삭제와 같은 조작은 디렉토리의 이름 접 두부를 공유하는 모든 오브젝트를 열거하고 처리하는 것이 아니라 디렉토리에서 단일 원자 메타 데이터 조작이됩니다. ADLS Gen2 계정에서 계층 네임 스페이스에 액세스하려면 파일 시스템을 초기화해야합니다.
# MAGIC
# MAGIC [Environment-Setup]($../Environment-Setup) 노트북은 두 가지 기능을 수행하여이 설정을 처리합니다:
# MAGIC
# MAGIC 1. OAuth 액세스를 사용하여 ADLS Gen2 (Azure Data Lake Storage Gen2)에 직접 연결을 구성합니다.
# MAGIC 2. `fs.azure.createRemoteFileSystemDuringInitialization` 구성 옵션을 사용하여 ADLS Gen2 파일 시스템을 초기화하여 파일 시스템이 처음 참조 될 때 파일 시스템을 만들 수 있도록합니다
# MAGIC
# MAGIC 다음 셀을 실행하기 전에 [여기]($ .. / Environment-Setup)의 노트북을 잠시 살펴보기 바랍니다.
# MAGIC
# MAGIC 노트북에서 다른 노트북을 실행할 수있는`% run` magic 명령을 사용하여 실행합니다.
# MAGIC
# MAGIC
# MAGIC > ** 중요 ** : 계층 네임 스페이스를 사용하도록 설정하면 Azure Blob Storage API를 사용할 수 없습니다. 즉,`wasb` 또는`wasbs` 체계를 사용하여`blob.core.windows.net` 엔드 포인트에 액세스 할 수 없습니다. 계층 적 네임 스페이스를 사용하면 ADLS Gen2 파일 시스템 (e.g., `abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net`)에 연결하기 위해 Azure Blob File System (ABFS) 구성표를 사용합니다.

# COMMAND ----------

# MAGIC %run "../Environment-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Cosmos DB
# MAGIC
# MAGIC
# MAGIC Azure Cosmos DB Spark 커넥터를 사용하면 이제 Cosmos DB를 입력 소스로 사용하여 Woodgrove의 트랜잭션 데이터 샘플을 검색합니다. Cosmos에 대한 정적 연결을 설정하고 거기에 저장된 트랜잭션 데이터 샘플을 읽는 것으로 시작합니다.
# MAGIC
# MAGIC Cosmos DB를 쿼리하려면 먼저 구성 정보가 포함 된 구성 개체를 만들어야합니다. 궁금한 점이 있으면 모든 옵션에 대한 자세한 내용은 [구성 참조] (https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references)를 참조하십시오.
# MAGIC
# MAGIC 제공해야 할 핵심 항목은 다음과 같습니다:
# MAGIC
# MAGIC   - **Endpoint**: Cosmos DB url (i.e. https://youraccount.documents.azure.com:443/).
# MAGIC   - **Masterkey**: Cosmos DB account의 primary 혹은 secondary key.
# MAGIC   - **Database**: database 이름.
# MAGIC   - **Collection**: Collection 이름.
# MAGIC
# MAGIC
# MAGIC > ** 참고 ** :이 실습에서는 이미 엔드 포인트 및 마스터 키를 Azure Key Vault에 추가 했으므로 `dbutils.secrets.get()`을 사용하여 값을 검색합니다. 다른 데이터베이스 및 콜렉션 이름을 사용하기로 선택한 경우, 실행하기 전에 아래 셀에있는 이름을 업데이트해야합니다.
# MAGIC
# MAGIC 구성의`query_custom` 속성은 Cosmos DB의 Woodgrove 트랜잭션 컬렉션에 대해 실행되는 쿼리입니다. 이 예에서는 트랜잭션 데이터의 작은 샘플 만 가져옵니다.
# MAGIC
# MAGIC 셀을 실행하여 Cosmos DB에 대한 정적 연결을 작성하는 데 필요한 구성을 추가하십시오.

# COMMAND ----------

# Cosmos DB Read Configuration
readConfig = {
  "Endpoint" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-URI"),
  "Masterkey" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-Key"),
  "Database" : "Woodgrove",
  "Collection" : "transactions",
  "SamplingRatio" : "1.0",
  "schema_samplesize" : "1000",
  "query_pagesize" : "2147483647",
  "query_custom" : "SELECT TOP 1000 * FROM c WHERE c.collectionType = 'Transaction'"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Read API를 사용하여 데이터를 쿼리하고 형식에 Cosmos DB를 지정하고 위에서 만든 구성 설정을 옵션으로 전달할 수 있습니다.

# COMMAND ----------

# Read transactions in from Cosmos DB via the Spark connector to create Spark DataFrame
static_transactions = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()

# COMMAND ----------

display(static_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC Cosmos DB에서 검색 한 데이터를 살펴보면 이전 노트북에서 살펴본 원시 데이터와 다른 몇 가지 사항을 알 수 있습니다.
# MAGIC
# MAGIC   1. 빈 필드와 검사 한 다른 열이 정리되었습니다. 이 실습의 시간과 단순성을 위해 Cosmos DB로 데이터를 전송하는 데 사용되는 트랜잭션 생성기는 다음 연습에서 생성 할 머신 학습 모델에서 사용하기 위해 이미 준비 및 정리 된 데이터를 사용하고 있습니다.
# MAGIC   2. 거래 날짜와 시간은 정수 값으로 separte 필드에 저장됩니다. 이러한 필드를보다 쉽게 사용할 수 있도록 준비된 데이터에서 단일 `transactionTimestamp`필드로 결합되었습니다.
# MAGIC   3. `_attachments`,`_etag` 및`_rid`와 같은 필드와 Cosmos DB가 문서에 추가하는 필드가 있습니다. 이 필드는 Cosmos DB 내부에 있으며 Databricks의 트랜잭션 데이터 분석에는 필요하지 않습니다. 아래에서이 필드를 제거합니다.

# COMMAND ----------

# Remove unwanted columns from the columns collection
cols = list(set(static_transactions.columns) - {'_attachments','_etag','_rid','_self','_ts','collectionType','id','ttl'})

static_transactions_clean = static_transactions.select(cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the transaction data to an Azure Databricks Delta table
# MAGIC
# MAGIC
# MAGIC [Databricks Delta] (https://docs.databricks.com/delta/delta-intro.html)는 Apache Spark 및 Databricks DBFS의 강력한 기능을 활용하여 강력한 트랜잭션 스토리지 계층을 제공합니다. 델타의 핵심 추상화는 다음과 같은 최적화 된 Spark 테이블입니다.
# MAGIC
# MAGIC   - DBFS에 데이터를 파켓 파일로 저장합니다.
# MAGIC   - 테이블 변경 사항을 효율적으로 추적하는 트랜잭션 로그를 유지 관리합니다.
# MAGIC
# MAGIC Hive 테이블 및 DBFS 디렉토리 작업에 사용하는 것과 동일한 익숙한 Apache Spark SQL 배치 및 스트리밍 API를 사용하여 델타 형식으로 저장된 데이터를 읽고 씁니다. 트랜잭션 로그와 기타 개선 사항이 추가되어 델타는 다음과 같은 중요한 이점을 제공합니다:
# MAGIC   - **ACID transactions**
# MAGIC     - 여러 작성자가 동시에 데이터 세트를 수정하고 일관된 뷰를 볼 수 있습니다. 이에대한 조건은 다중 클러스터 쓰기를 참조하십시오.
# MAGIC     - 작성자는 데이터 세트를 읽는 작업을 방해하지 않고 데이터 세트를 수정할 수 있습니다.
# MAGIC   - **Fast read access**
# MAGIC     - 자동 파일 관리는 데이터를 효율적으로 읽을 수있는 큰 파일로 구성합니다.
# MAGIC     - 통계를 통해 읽기 속도가 10-100 배 빨라지고 데이터 건너 뛰기로 관련없는 정보를 읽지 않습니다.
# MAGIC     
# MAGIC 트랜잭션 델타 테이블을 만들려면 먼저 `static_transactions_clean` DataFrame에 포함 된 정리 된 데이터 집합을 ADbris Gen2의 폴더에 Databricks Delta 형식으로 씁니다.
# MAGIC
# MAGIC 명령을 실행하기 전에 아래 셀의 명령을 분석해 봅시다.
# MAGIC   - `mode("overwrite")`: 이는 지정된 위치에 저장된 기존 델타 테이블을 덮어 쓰도록 쓰기 작업에 지시합니다.
# MAGIC   - `format("delta")`: 델타 형식으로 데이터를 저장하려면`write` 명령의`format ()`옵션과 함께 "delta"를 지정하십시오.
# MAGIC   - `partitionBy()`: 새 델타 테이블을 생성 할 때 파티션 열을 선택적으로 지정할 수 있습니다. 파티셔닝은 파티션 열과 관련된 술어가있는 쿼리 또는 DML의 속도를 높이는 데 사용됩니다. 이 경우, 우리는`ipCountryCode`에서 파티셔닝하고 있는데, 이는 Cosmos DB에 저장된 데이터를 파티셔닝하는 데 사용되는 것과 동일한 필드이기도합니다.
# MAGIC   - `save()`: `save` 명령은 델타 테이블의 기본 파일이 저장 될 위치를 받아들입니다. 우리의 경우, abfs URI를 사용하여 제공 할 ADLS Gen2 파일 시스템의 위치입니다.

# COMMAND ----------

static_transactions_clean.write.mode("overwrite").format("delta").partitionBy("ipCountryCode").save("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/delta/transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC 정리 된 트랜잭션 데이터를 델타 형식의 ADLS Gen2 파일 시스템 위치에 저장 했으므로 위에서 만든 델타 위치로 지원되는 Databricks 전역 테이블을 만들 수 있습니다. `CREATE TABLE`쿼리에 지정된 `LOCATION`은 위의 델타 형식으로 정리 된 트랜잭션 데이터를 쓰는 데 사용한 것과 동일합니다. 이렇게하면 Hive 메타 스토어의 테이블이 기존 데이터의 스키마, 파티셔닝 및 테이블 특성을 자동으로 상속 할 수 있습니다.
# MAGIC
# MAGIC ** 중요 ** : 셀을 실행하기 전에 ADLS Gen2 계정 이름을 아래 위치 값에 추가해야합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://transactions@<your-adls-gen2-account-name>.dfs.core.windows.net/delta/transactions' -- TODO: <your-adls-gen2-account-name>부분을 ADLS Gen2 계정 이름으로 교체해야 합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE DETAIL` SQL 명령을 사용하여 스키마, 파티셔닝, 테이블 크기 등에 대한 정보를 볼 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL transactions

# COMMAND ----------

# MAGIC %md
# MAGIC 마지막으로 Spark SQL을 사용하여 Hive 테이블의 레코드를 쿼리 할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM transactions LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream transactions from Cosmos DB Change Feed into Azure Databricks
# MAGIC
# MAGIC
# MAGIC 이제 Cosmos DB에서 정적 데이터를 읽지 말고 Cosmos DB 변경 피드를 사용하여 스트리밍 연결을 설정하겠습니다. Spark Structured Streaming을 사용하여 Azure Databricks Delta 테이블로 데이터를 스트리밍합니다.
# MAGIC
# MAGIC Azure Cosmos DB의 변경 피드 지원은 모든 변경 사항에 대해 Azure Cosmos DB 컨테이너를 수신하여 작동합니다. 그런 다음 수정 된 순서대로 변경된 정렬 된 문서 목록을 출력합니다. 변경 사항이 유지되고 비동기식 및 증 분식으로 처리 될 수 있으며 병렬 처리를 위해 하나 이상의 소비자에게 출력을 분산시킬 수 있습니다.
# MAGIC
# MAGIC 아래 셀에서 Cosmos DB 변경 피드에 연결하는 데 필요한 정보가 포함 된 구성 오브젝트가 작성됩니다.
# MAGIC
# MAGIC 이 구성은 Cosmos DB 변경 피드의 레코드를 DataFrame으로 스트리밍하기 위해 구조적 스트리밍의`readStream` 명령과 함께 사용됩니다.
# MAGIC
# MAGIC > ** 참고 ** : 아래에 사용 된`format` 속성은 Cosmos DB에서 스트리밍 데이터를 읽을 때 배치 또는 정적 데이터를 읽는 것과 다릅니다. 스트리밍 데이터의 경우 `com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSourceProvider`를 사용합니다.

# COMMAND ----------

# Read Configuration
changeFeedConfig = {
  "Endpoint" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-URI"),
  "Masterkey" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-Key"),
  "Database" : "Woodgrove",
  "Collection" : "transactions",
  "ReadChangeFeed" : "true",
  "ChangeFeedQueryName" : "cosmos-change-feed-transactions-query",
  "ChangeFeedStartFromTheBeginning" : "false",
  "ChangeFeedUseNextToken" : "true",
  "ChangeFeedCheckpointLocation" : "/cosmos/_checkpoints/transactions",
  "SamplingRatio" : "1.0"
}

# Open a read stream to the Cosmos DB Change Feed via azure-cosmosdb-spark to create Spark DataFrame
changes = (spark
           .readStream
           .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSourceProvider")
           .options(**changeFeedConfig)
           .load())

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame에는`isStreaming` 속성이 있으며, 위에서 만든`changes` DataFrame이 스트리밍 DataFrame인지 확인하는 데 사용할 수 있습니다.

# COMMAND ----------

changes.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC 비교를 위해 정적 트랜잭션 DataFrame의`isStreaming` 속성을 확인 해봅니다.

# COMMAND ----------

static_transactions.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare the data
# MAGIC
# MAGIC 구조적 스트리밍 (Structured Streaming)은 정적 데이터에서 배치 계산을 표현하는 것과 같은 방식으로 스트리밍 데이터에서 계산을 표현할 수있는 Apache Spark API입니다. Spark SQL 엔진은 스트리밍 데이터가 도착함에 따라 점차적으로 계산을 수행하여 결과를 업데이트합니다.
# MAGIC
# MAGIC 위에서 언급했듯이 Cosmos DB에서 불필요한 열을 제거하여 수신 데이터 세트를 준비하려고합니다.
# MAGIC
# MAGIC `changes_clean` DataFrame은`changes` DataFrame에서`isStreaming` 속성을 상속합니다.

# COMMAND ----------

# Remove unwanted columns from the columns collection
cols = list(set(changes.columns) - {'_attachments','_etag','_rid','_self','_ts','collectionType','id','ttl'})

changes_clean = changes.select(cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the TransactionGenerator
# MAGIC
# MAGIC
# MAGIC 스트리밍 쿼리가 생성되면 트랜잭션 생성기를 다시 시작할 차례입니다. 실습 랩 단계별 가이드 Exercise 1에서 LabVM의 `TransactionGenerator` 콘솔 앱을 사용하여 Cosmos DB로 트랜잭션을 보냅니다. 이러한 트랜잭션은 새 트랜잭션이 Comsos DB 콜렉션에 삽입 될 때 Cosmos DB 변경 피드에서 읽을 수 있도록하는 데 사용됩니다.
# MAGIC
# MAGIC 1. LabVM 및 TransactionGenerator 콘솔 앱으로 돌아갑니다.
# MAGIC 2. Visual Studio 도구 모음에서 녹색 ** Run ** 버튼을 선택하여 콘솔 앱을 시작합니다.
# MAGIC 3. 콘솔 앱이 실행되면이 노트북으로 돌아가서 다음 셀로 계속 진행하십시오.
# MAGIC
# MAGIC ## Write the incoming streamed data to an Azure Databricks Delta table stored in ADLS Gen2
# MAGIC
# MAGIC `transactions` 델타 테이블에 스트리밍 데이터 쓰기를 시작하려면 아래 셀을 실행하십시오.
# MAGIC
# MAGIC 명령의 구성 요소를 빠르게 살펴 봅시다:
# MAGIC   - `writeStream`: 이 옵션은 스트리밍 데이터를 출력 위치에 쓸 때 사용됩니다.
# MAGIC   - `format("delta")`: 이것은 들어오는 스트림 데이터를 델타 형식으로 쓰고 있음을 기록기에 알려줍니다.
# MAGIC   - `outputMode("append")`: 수신 데이터를 기존 델타 테이블에 추가하려고 하므로 이를 "append"로 설정합니다.
# MAGIC   - `option("checkpointLocation")`: CheckPoint 위치를 지정하면 중단 된 위치에서 writeStream 작업을 중단 할 수 있습니다.
# MAGIC   - `table("transactions")`: 테이블 옵션을 사용하면 쓰기 작업의 대상 테이블을 지정할 수 있습니다.

# COMMAND ----------

changeFeed = (changes_clean
 .writeStream
 .format("delta")
 .queryName("change-feed")
 .outputMode("append")
 .option("checkpointLocation", "/delta/_checkpoints/transactions/")
 .table("transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine streaming query output
# MAGIC
# MAGIC 위 셀의 출력에서 녹색 스트리밍 아이콘의 오른쪽에있는 드롭 다운 화살표를 선택하여 스트리밍 쿼리를 확장하십시오. 출력에는 대시 보드보기 및 원시 데이터보기가 제공됩니다.
# MAGIC
# MAGIC ![Streaming query output charts](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-structured-streaming-query-output.png "Streaming query output charts")
# MAGIC
# MAGIC ** Dashboard ** 에서는 출력이 들어오는 스트리밍 데이터에 대한 정보를 제공하는 차트로 표시됩니다. ** Raw Data **를 선택하고 세부 정보를 텍스트 형식으로 볼 수도 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count records in the `transactions` table
# MAGIC
# MAGIC 아래의 SQL 카운트 쿼리를 실행하여 데이터가 `transactions` 델타 테이블에 기록되는 방식을 관찰 할 수 있습니다. Cosmos DB 변경 피드 스트림에서 테이블에 새 레코드가 추가 될 때마다 10 또는 15 초마다이 쿼리를 다시 실행하십시오.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the streaming query
# MAGIC
# MAGIC TransactionGenerator가 완료 될 때까지 실행 한 다음 아래 셀을 실행하여 스트리밍 쿼리를 중지하십시오.

# COMMAND ----------

changeFeed.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Exercise 2와 관련된 작업을 완료했습니다. Cosmos DB real-time advanced analytics hands-on lab setup guide로 돌아가서 Exercise 3으로 계속 진행하십시오.
