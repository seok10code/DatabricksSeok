# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 4 - Task 4: Using an Azure Databricks job to batch score on a schedule
# MAGIC
# MAGIC 이 전자 필기장은 Azure Databricks Job에 의해 호출됩니다. 다음 단계를 수행합니다.
# MAGIC
# MAGIC   1. 훈련 된 ML 모델을 DBFS에서로드합니다.
# MAGIC   2. `transactions` 델타 테이블에서 배치 데이터를 채점합니다.
# MAGIC   3. 새로 채점 된 데이터를`scored_transactions` 델타 테이블에 추가합니다.
# MAGIC   4. Cosmos DB Spark 커넥터를 사용하여`isSuspicious`가 true 인 레코드를 Cosmos DB에 씁니다.
# MAGIC
# MAGIC > ** 중요 ** :이 노트북은 **Job**에 의해 실행됩니다. **이 실습을 위해 이 노트북의 셀을 수동으로 실행하지 마십시오.**

# COMMAND ----------

dbutils.library.installPyPI('azureml-sdk', extras='automl_databricks')
dbutils.library.installPyPI('scikit-learn', '0.21.1')

#best practice is to restart python after installing libraries
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "../Environment-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load ML model from DBFS
# MAGIC
# MAGIC 배치 스코어링을 수행하려면 먼저 Databricks Job과 함께 사용하기 위해 DBFS에 저장할 때 지정한 DBFS 위치에서 모델을 로드 해야합니다.

# COMMAND ----------

# Import the required libraries
import numpy
import os
import pandas as pd
from sklearn.externals import joblib

model_folder_path="models"
model_name="batch-score"

target_dir = '/FileStore/' + model_folder_path
if not os.path.exists(target_dir):
    os.makedirs(target_dir)

# Specify the directory where the model was saved.
adls_dir = "abfss://" + fileSystemName + "@" + adlsGen2AccountName + ".dfs.core.windows.net/models/"

# Copy the model from ADLS to the Spark Driver, so it can be loaded.
dbutils.fs.cp(adls_dir+ '/' + model_name + '.pkl', 'file:///' + target_dir, True)

model_file_path = os.path.join(target_dir, model_name + '.pkl')
print('Loading model from:', model_file_path)
model = joblib.load(model_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch score transactions
# MAGIC
# MAGIC 이제 모델이 로드 된 상태에서 다음 단계는 Databricks Delta로 가져온 트랜잭션이 포함 된 DataFrame을 만들고 모델을 사용하여 각 레코드의 점수를 매기는 것입니다. 이전 연습에서했던 것처럼 모델이 사용할 수 있도록 `transactions` 테이블에서 해당 데이터를 변환해야합니다. 파이프 라인에서 사용하기 위해 다음과 같이 변환을 custom transformers로 인코딩하십시오.

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
# MAGIC 배치 데이터가 변환되면 이제 ML 모델을 사용하여 데이터 세트의 각 트랜잭션이 의심 스러운지 예측할 수 있습니다. 다음 셀을 실행하여 모델에서 예측을 검색하십시오.

# COMMAND ----------

transactions_preds = model.predict(preprocessed_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC 트랜잭션 결과에 예측 결과를 추가하려면 배열에서`tolist ()`메소드를 사용할 수 있습니다. 그러면 Pandas DataFrame의 각 행 열에 순서대로 할당됩니다. 이 경우 예측을 `isSuspicious`라는 새 열로 추가합니다.

# COMMAND ----------

pandas_df["isSuspicious"] = transactions_preds.tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC 스코어링 된 트랜잭션 데이터를 Databricks Delta 테이블에 쓸 수있게하려면 Pandas DataFrame을 Spark DataFrame으로 다시 변환하십시오.

# COMMAND ----------

scored_transactions = spark.createDataFrame(pandas_df)

# COMMAND ----------

# Convert columns to their appropriate types before merging into the Delta table
from pyspark.sql.functions import col , column
scored_transactions = scored_transactions.withColumn("accountAge", col("accountAge").cast("int"))
scored_transactions = scored_transactions.withColumn("digitalItemCount", col("digitalItemCount").cast("int"))
scored_transactions = scored_transactions.withColumn("physicalItemCount", col("physicalItemCount").cast("int"))
scored_transactions = scored_transactions.withColumn("numPaymentRejects1dPerUser", col("numPaymentRejects1dPerUser").cast("int"))
scored_transactions = scored_transactions.withColumn("localHour", col("localHour").cast("int"))

# COMMAND ----------

scored_transactions.write.mode("append").format("delta").save("dbfs:/delta/scored_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data to Cosmos DB
# MAGIC
# MAGIC Woodgrove는 추가 작업을 위해 ML 모델에서 `isSuspicious`로 표시된 레코드를 Cosmos DB에 기록하도록 요청했습니다.
# MAGIC
# MAGIC 원하는 변경 내용이 포함 된 DataFrame을 만들어 Cosmos DB에 데이터를 다시 쓴 다음 Write API를 사용하여 변경 내용을 다시 저장합니다. 쓰기 API에는 이전에 사용한 읽기 API와 유사한 구성이 필요합니다. 여기에 사용 된`Upsert` 매개 변수를 기록하여 Cosmos DB 측에서 Upsert를 가능하 합니다.

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

# COMMAND ----------

# MAGIC %md
# MAGIC Save the data to Cosmos DB.

# COMMAND ----------

suspicious_transactions.write.format("com.microsoft.azure.cosmosdb.spark").mode("overwrite").options(**writeConfig).save()
