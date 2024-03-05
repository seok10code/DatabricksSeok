# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 4 - Task 3: Distributing batch scored data globally using Cosmos DB
# MAGIC
# MAGIC 이 노트북에서는 `transactions` 델타 테이블에서 배치 트랜잭션을 검색하고 훈련 된 기계 학습 모델을 사용하여 각 트랜잭션이 의심 스러운지 여부를 예측합니다. 그런 다음 모델 예측을 `isSuspicious`라는 열의 트랜잭션 데이터에 추가하고이를 `scored_transactions` 델타 테이블에 저장합니다.
# MAGIC
# MAGIC 또한 `isSuspicious`가 true 인 모든 트랜잭션을 선택하고 Comsos DB의 트랜잭션 컬렉션에 기록합니다.
# MAGIC
# MAGIC 마지막으로, `scored_transactions` 데이터를 사용하여 의심스러운 각 `ipCountryCode`에 대한 트래픽의 백분율을 판별하기 위해 데이터 집계를 수행하고 이를 Exercise 5에서 Power BI가 사용할 수 있도록 다른 델타 테이블에 저장합니다.
# MAGIC
# MAGIC 이 노트북에서는 [Azure Cosmos DB Spark Connector](https://github.com/Azure/azure-cosmosdb-spark)를 사용하여 Cosmos DB에 대한 연결을 만들고 점수가 매겨진 거래 데이터를 Cosmos DB 컬렉션에 씁니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach notebook to your cluster
# MAGIC
# MAGIC 이전 노트북에서와 같이 셀을 실행하기 전에이 노트북을 클러스터에 연결해야합니다. 노트북의 도구 모음에서 드롭 다운 화살표를 선택한 다음 연결 대상에서 클러스터를 선택하십시오.
# MAGIC
# MAGIC ![Detach is expanded in the notebook toolbar, and the cluster is highlighted under Attach to.](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-attach-notebook.png "Attach notebook")
# MAGIC
# MAGIC ## Environment setup
# MAGIC `Environment-Setup` 노트북을 실행하여 ADLS Gen2 (Azure Data Lake Storage Gen2)에 직접 연결합니다.
# MAGIC

# COMMAND ----------

dbutils.library.installPyPI('azureml-sdk', extras='automl_databricks')
dbutils.library.installPyPI('scikit-learn', '0.21.1')

#best practice is to restart python after installing libraries
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "../Environment-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load ML model
# MAGIC
# MAGIC 마지막 연습에서는 훈련 된 모델을 Azure ML 작업 영역에 저장했습니다. 배치 스코어링 수행하려면 먼저 Azure ML 작업 영역에서 모델을 로드해야 합니다. 아래 셀에서 Azure ML 작업 영역을 검색하고 거기에 저장된 모델을 로드하기 위한 일부 Helper Function을 정의합니다.

# COMMAND ----------

# Import the required libraries
import numpy
import os
import pandas as pd
import azureml
from azureml.core import Workspace
from azureml.core.model import Model
from sklearn.externals import joblib

def getOrCreateWorkspace(subscription_id, resource_group, workspace_name, workspace_region):
    # By using the exist_ok param, if the worskpace already exists we get a reference to the existing workspace instead of an error
    ws = Workspace.create(
        name = workspace_name,
        subscription_id = subscription_id,
        resource_group = resource_group, 
        location = workspace_region,
        exist_ok = True)
    return ws

def loadModelFromAML(ws, model_name="batch-score"):
  # download the model folder from AML to the current working directory
  model_folder_path = Model.get_model_path(model_name, _workspace=ws)
  model_file_path = os.path.join(model_folder_path, model_name + '.pkl')
  print('Loading model from:', model_file_path)
  model = joblib.load(model_file_path)
  return model

# COMMAND ----------

# MAGIC %md
# MAGIC 아래 셀을 실행하여 모델을로드하십시오. 출력의 프롬프트에 응답하여 <https://microsoft.com/devicelogin>으로 이동 한 다음 아래에 지정된 코드를 입력하여 인증해야합니다.
# MAGIC
# MAGIC 배치 스코어링 모델을 저장할 때 이전 노트의 맨 아래에 제공 한 것과 **동일한 값**을 입력하십시오.

# COMMAND ----------

# Test loading the model
#Provide the Subscription ID of your existing Azure subscription
subscription_id = #"YOUR_SUBSCRIPTION_ID"

#Provide values for the Resource Group and Workspace that you previously created
resource_group = #"YOUR_RESOURCE_GROUP"
workspace_name = "aml-workspace"
workspace_region = 'koreacentral' # eastus, westcentralus, southeastasia, australiaeast, westeurope

#Get an AML Workspace
ws =  getOrCreateWorkspace(subscription_id, resource_group, 
                   workspace_name, workspace_region)

model = loadModelFromAML(ws)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save ML model to DBFS
# MAGIC
# MAGIC 다음 작업에서는 일정에 따라 배치 점수를 실행하기 위해 Databricks 작업을 생성합니다. 이 스케줄 된 작업이 모델을 쉽게 사용할 수 있도록 모델의 사본을 Databricks 작업 공간 내 DBFS의 공유 폴더에 저장하는 것이 가장 쉽습니다. Databricks는 예약 된 작업이 실행될 때마다 새 클러스터를 가동 시키므로 매번 발생할 때마다 작업이 AML 작업 영역에 대해 인증 될 필요가 없습니다.

# COMMAND ----------

model_folder_path="models"
model_name="batch-score"

target_dir = '/FileStore/' + model_folder_path
if not os.path.exists(target_dir):
    os.makedirs(target_dir)

# create a directory to save the model into.
dbutils.fs.mkdirs("abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/models")
adls_dir = "abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/models/"

# save the model to disk
joblib.dump(model, target_dir + '/' + model_name + '.pkl')

# joblib.dump saves the file to the local file system on the Spark Driver node, so next you need to copy it to your ADLS Gen2 filesystem so it can be accessed by other clusters.
dbutils.fs.cp('file:///' + target_dir + '/' + model_name + '.pkl', adls_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch score transactions
# MAGIC
# MAGIC 이제 모델이 로드 된 상태에서 다음 단계는 Databricks Delta로 가져온 트랜잭션이 포함 된 DataFrame을 만들고 모델을 사용하여 각 레코드의 점수를 매기는 것입니다. 
# MAGIC 이전 연습에서했던 것처럼 모델이 사용할 수 있도록 `transactions` 테이블에서 해당 데이터를 변환해야합니다. 파이프 라인에서 사용하기 위해 다음과 같이 변환을 Custom Transformer로 인코딩하십시오.

# COMMAND ----------

from sklearn.base import BaseEstimator, TransformerMixin
class NumericCleaner(BaseEstimator, TransformerMixin):
    def __init__(self):
        self = self
    def fit(self, X, y=None):
        print("NumericCleaner.fit called")
        return self
    def transform(self, X):
        print("NumericCleaner.transform called")
        X["localHour"] = X["localHour"].fillna(-99)
        X["accountAge"] = X["accountAge"].fillna(-1)
        X["numPaymentRejects1dPerUser"] = X["numPaymentRejects1dPerUser"].fillna(-1)
        X.loc[X.loc[:,"localHour"] == -1, "localHour"] = -99
        return X

class CategoricalCleaner(BaseEstimator, TransformerMixin):
    def __init__(self):
        self = self
    def fit(self, X, y=None):
        print("CategoricalCleaner.fit called")
        return self
    def transform(self, X):
        print("CategoricalCleaner.transform called")
        X = X.fillna(value={"cardType":"U","cvvVerifyResult": "N"})
        X['isUserRegistered'] = X.apply(lambda row: 1 if row["isUserRegistered"] == "TRUE" else 0, axis=1)
        return X

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder

numeric_features=["transactionAmountUSD", "localHour", 
                  "transactionIPaddress", "digitalItemCount", "physicalItemCount", "accountAge",
                  "paymentInstrumentAgeInAccount", "numPaymentRejects1dPerUser"
                 ]

categorical_features=["transactionCurrencyCode", "browserLanguage", "paymentInstrumentType", "cardType", "cvvVerifyResult",
                      "isUserRegistered"
                     ]                           

numeric_transformer = Pipeline(steps=[
    ('cleaner', NumericCleaner())
])
                               
categorical_transformer = Pipeline(steps=[
    ('cleaner', CategoricalCleaner()),
    ('encoder', OrdinalEncoder())])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 배치 `transaction` 데이터를 Spark DataFrame에로드하고 Pandas DataFrame으로 변환한 다음 해당 데이터를 변환 파이프 라인을 통해 전달합니다.

# COMMAND ----------

# Load transactions from the Delta table into a Spark DataFrame
transactions = spark.sql("SELECT * FROM transactions")

# Get a Pandas DataFrame from the Spark DataFrame
pandas_df = transactions.toPandas()

# Transform the batch data
preprocessed_transactions = preprocessor.fit_transform(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score the batch data
# MAGIC
# MAGIC 배치 데이터가 변환되면 이제 ML 모델을 사용하여 데이터 세트의 각 트랜잭션이 의심 스러운지 예측할 수 있습니다. 다음 셀을 실행하여 모델에서 예측을 수행하십시오.

# COMMAND ----------

transactions_preds = model.predict(preprocessed_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC 아래의 셀을 실행하여 예측을 봅니다. 출력은 `array`형식입니다. 배열의 각 항목은 레코드 순서에 따라 `transactions` 배치 데이터의 레코드와 연결됩니다

# COMMAND ----------

transactions_preds

# COMMAND ----------

# MAGIC %md
# MAGIC 트랜잭션 결과에 예측 결과를 추가하려면 배열에서`tolist ()`메소드를 사용할 수 있습니다. 그러면 Pandas DataFrame의 각 행 열에 순서대로 할당됩니다. 이 경우 예측을 `isSuspicious`라는 새 열로 추가합니다.

# COMMAND ----------

pandas_df["isSuspicious"] = transactions_preds.tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 데이터 세트에서 의심스러운 레코드와 의심스럽지 않은 레코드의 수를 빠르게 살펴볼 수 있습니다.

# COMMAND ----------

pandas_df['isSuspicious'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC 스코어링 된 트랜잭션 데이터를 Databricks Delta 테이블에 쓸 수있게하려면 Pandas DataFrame을 Spark DataFrame으로 다시 변환하십시오.

# COMMAND ----------

scored_transactions = spark.createDataFrame(pandas_df)
display(scored_transactions)

# COMMAND ----------

# Convert columns to their appropriate types before merging into the Delta table
from pyspark.sql.functions import col , column
scored_transactions = scored_transactions.withColumn("accountAge", col("accountAge").cast("int"))
scored_transactions = scored_transactions.withColumn("digitalItemCount", col("digitalItemCount").cast("int"))
scored_transactions = scored_transactions.withColumn("physicalItemCount", col("physicalItemCount").cast("int"))
scored_transactions = scored_transactions.withColumn("numPaymentRejects1dPerUser", col("numPaymentRejects1dPerUser").cast("int"))
scored_transactions = scored_transactions.withColumn("localHour", col("localHour").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 DataFrame을 Delta 테이블에 씁니다. 이 테이블의 경우보고 및 시각화를 위해 Power BI가 액세스하므로 위치는 ADLS Gen2가 아니라 DBFS에 있습니다. 현재 Power BI에서 ADLS에 로드 할 때 ADLS에있는 델타 테이블에 액세스하는 데 인증 문제가 있습니다.

# COMMAND ----------

scored_transactions.write.mode("overwrite").format("delta").partitionBy("ipCountryCode").save("dbfs:/delta/scored_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC 이전에 `transactions` 델타 테이블에서했던 것처럼 델타 위치를 가리키는 Databricks 글로벌 테이블을 작성하면 테이블을 더 쉽게 쿼리 할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE scored_transactions
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/delta/scored_transactions'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scored_transactions LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to Cosmos DB
# MAGIC
# MAGIC Woodgrove는 추가 작업을 위해 ML 모델에서 `isSuspicious`로 표시된 레코드를 Cosmos DB에 기록하도록 요청했습니다.
# MAGIC
# MAGIC 원하는 변경내용이 포함 된 DataFrame을 만들어 Cosmos DB에 데이터를 다시 쓴 다음 Write API를 사용하여 변경 내용을 다시 저장합니다. 쓰기 API에는 이전에 사용한 읽기 API와 유사한 구성이 필요합니다. 여기에 사용 된`Upsert` 매개 변수를 기록하여 Cosmos DB 측에서 Upsert를 가능하게 합니다.

# COMMAND ----------

# Cosmos DB write configuration
writeConfig = {
  "Endpoint" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-URI"),
  "Masterkey" : dbutils.secrets.get(keyVaultScope, "Cosmos-DB-Key"),
  "Database" : "Woodgrove",
  "Collection" : "transactions",
  "Upsert" : "true"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL을 사용하여`suspicious_transactions` DataFrame을 만듭니다. Cosmos DB에 트랜잭션을 저장하는 데 사용하는 것과 동일한 컬렉션에이 레코드를 작성하므로 문서 유형을 구별하기 위해`collectionType`을 "Suspicious_Transaction"으로 지정하기 위해 새 열을 삽입합니다.

# COMMAND ----------

suspicious_transactions = spark.sql("SELECT *, 'SuspiciousTransactions' AS collectionType FROM scored_transactions WHERE isSuspicious = 1")
display(suspicious_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC 기본적으로 Cosmos DB 커넥터를 사용하면 이미 동일 키의 문서가있는 컬렉션에 쓸 수 없습니다. upsert를 수행하려면 mode가 `overwrite`로 설정되어 있어야합니다.

# COMMAND ----------

suspicious_transactions.write.format("com.microsoft.azure.cosmosdb.spark").mode("overwrite").options(**writeConfig).save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate data by `ipCountryCode`
# MAGIC
# MAGIC 이 노트북에서 수행 할 마지막 작업은 `isSuspicious`값과 `ipCountryCode`를 기반으로 간단한 데이터 집계를 수행하는 것입니다.

# COMMAND ----------

suspicious_agg = spark.sql("SELECT ipCountryCode, COUNT(CASE WHEN isSuspicious = 1 THEN 0 END) SuspiciousTransactionCount, COUNT(*) AS TotalTransactionCount, COUNT(CASE WHEN isSuspicious = 1 THEN 0 END)/COUNT(*) AS PercentSuspicious FROM scored_transactions GROUP BY ipCountryCode ORDER BY ipCountryCode")

# COMMAND ----------

from pyspark.sql.functions import *

display(suspicious_agg.orderBy(desc('PercentSuspicious')))

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 DataFrame을 Delta 테이블에 씁니다.

# COMMAND ----------

suspicious_agg.write.mode("overwrite").format("delta").save("dbfs:/delta/percent_suspicious")

# COMMAND ----------

# MAGIC %md
# MAGIC 다른 델타 테이블과 마찬가지로 델타 위치를 가리키는 Databricks 글로벌 테이블을 작성하면 테이블을 더 쉽게 쿼리 할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE percent_suspicious
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/delta/percent_suspicious'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM percent_suspicious

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC `transactions` 데이터에 대해 배치 스코어링을 성공적으로 수행하고 스코어링 된 트랜잭션을 ADLS Gen2 파일 시스템에있는 Databricks Delta 테이블에 저장했습니다.
# MAGIC
# MAGIC Cosmos DB real-time advanced analytics hands-on lab setup guide 로 돌아가서 Exercise 4 Task 2를 계속합니다.
