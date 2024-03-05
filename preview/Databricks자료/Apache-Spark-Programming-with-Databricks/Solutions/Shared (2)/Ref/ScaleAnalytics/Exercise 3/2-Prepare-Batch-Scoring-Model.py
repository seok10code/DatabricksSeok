# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3 - Task 2: Prepare batch scoring model
# MAGIC
# MAGIC 이 노트북에서는 배치 스코어링에 사용될 의심스러운 활동을 감지하는 데 사용되는 모델을 준비합니다.

# COMMAND ----------

# MAGIC %md # Load Data Sets

# COMMAND ----------

dbutils.library.installPyPI('azureml-sdk', extras='automl_databricks')
dbutils.library.installPyPI('scikit-learn', '0.21.1')

dbutils.library.installPyPI('numpy','1.16.3')
dbutils.library.installPyPI('pandas','0.24.2')

# COMMAND ----------

#best practice is to restart python after installing libraries
dbutils.library.restartPython()

# COMMAND ----------

dbutils.library.list()

# COMMAND ----------

# MAGIC %md Woodgrove 은행 팀은 모델에 대한 훈련을 위해 내 보낸 데이터의 CSV 사본을 내보냈습니다. 다음 셀을 실행하여 데이터 세트를 다운로드하십시오.

# COMMAND ----------

import uuid
import os

# Create a temporary folder to store locally relevant content for this notebook
tempFolderName = '/FileStore/mcw_cdb_{0}'.format(uuid.uuid4())
dbutils.fs.mkdirs(tempFolderName)
print('Content files will be saved to {0}'.format(tempFolderName))

filesToDownload = ['Account_Info.csv', 'Fraud_Transactions.csv', 'Untagged_Transactions.csv']

for fileToDownload in filesToDownload:
  downloadCommand = 'wget -O ''/dbfs{0}/{1}'' ''https://databricksdemostore.blob.core.windows.net/data/mcw-cdb/{1}'''.format(tempFolderName, fileToDownload)
  print(downloadCommand)
  os.system(downloadCommand)
  
#List all downloaded files
dbutils.fs.ls(tempFolderName)

# COMMAND ----------

# MAGIC %md 다음 셀을 실행하여 필요한 라이브러리를 가져옵니다.

# COMMAND ----------

import pandas as pd
import numpy

# COMMAND ----------

# MAGIC %md 다음 셀을 실행하여 CSV 파일을 데이터 프레임으로로드하십시오.
# MAGIC
# MAGIC 참고 : 여기에 사용 된 데이터에 대한 스키마 문서는 다음 위치에서 제공됩니다.
# MAGIC https://microsoft.github.io/r-server-fraud-detection/input_data.html

# COMMAND ----------

fraud_df = pd.read_csv(os.path.join('/dbfs' + tempFolderName, 'Fraud_Transactions.csv'))
account_df = pd.read_csv(os.path.join('/dbfs' + tempFolderName, 'Account_Info.csv'), low_memory=False)
untagged_df = pd.read_csv(os.path.join('/dbfs' + tempFolderName, 'Untagged_Transactions.csv'), low_memory=False)

# COMMAND ----------

# MAGIC %md 클러스터에 필요한 라이브러리가 있는지 확인해야 합니다:
# MAGIC - Scikit-Learn version 0.21.1 or above
# MAGIC - Azure Machine Learning 1.0.39 or above

# COMMAND ----------

import sklearn
import azureml.core

print("Scikit-Learn: ", sklearn.__version__)
print("AzureML: ", azureml.core.VERSION)

# COMMAND ----------

# MAGIC %md # Prepare Accounts

# COMMAND ----------

# MAGIC %md 
# MAGIC 계정 데이터 세트의 데이터를 정리하여 시작합니다.
# MAGIC `accountOwnerName`,`accountAddress`,`accountCity` 및`accountOpenDate`와 같은 값이 거의 혹은 전혀 없는 열을 제거합니다.

# COMMAND ----------

account_df_clean = account_df[["accountID", "transactionDate", "transactionTime", 
                               "accountPostalCode", "accountState", "accountCountry", 
                               "accountAge", "isUserRegistered", "paymentInstrumentAgeInAccount", 
                               "numPaymentRejects1dPerUser"]]
account_df_clean = account_df_clean.copy()

# COMMAND ----------

# MAGIC %md `paymentInstrumentAgeInAccount`을 정리합니다. 숫자가 아닌 값 (예: 잘못된 문자열 값 또는 Garbage 데이터)이 NaN으로 변환되었는지 확인한 다음 해당 NaN 값을 0으로 채울 수 있습니다.

# COMMAND ----------

account_df_clean['paymentInstrumentAgeInAccount'] = pd.to_numeric(account_df_clean['paymentInstrumentAgeInAccount'], errors='coerce')
account_df_clean['paymentInstrumentAgeInAccount'] = account_df_clean[['paymentInstrumentAgeInAccount']].fillna(0)['paymentInstrumentAgeInAccount']

# COMMAND ----------

# MAGIC %md 
# MAGIC 다음으로`numPaymentRejects1dPerUser`를 변환하여 열에`object` 대신`float`의 데이터 유형이 있도록 합니다.

# COMMAND ----------

account_df_clean["numPaymentRejects1dPerUser"] = account_df_clean[["numPaymentRejects1dPerUser"]].astype(float)["numPaymentRejects1dPerUser"]
account_df_clean["numPaymentRejects1dPerUser"].value_counts()

# COMMAND ----------

# MAGIC %md 
# MAGIC `transactionDate` 및`transactionTime` 필드를 단일 필드`transactionDateTime`으로 결합해야합니다. transactionTime을 정수에서 hhmmss (2 자리 시간 분 초) 형식의 0으로 채워진 6 자리 문자열로 변환 한 다음 두 열을 연결하고 마지막으로 연결된 문자열을 DateTime 값으로 Parsing 합니다.

# COMMAND ----------

account_df_clean["transactionTime"] = ['{0:06d}'.format(x) for x in account_df_clean["transactionTime"]]
account_df_clean["transactionDateTime"] = pd.to_datetime(account_df_clean["transactionDate"].map(str) + account_df_clean["transactionTime"], format='%Y%m%d%H%M%S')
account_df_clean["transactionDateTime"]

# COMMAND ----------

account_df_clean.info()

# COMMAND ----------

# MAGIC %md `account_df_clean` 은 사용할 준비가 되었습니다.

# COMMAND ----------

# MAGIC %md # Prepare Untagged Transactions

# COMMAND ----------

# MAGIC %md untagged_transactions에는 값이 모두 널인 열이 16 개 있습니다. 데이터 세트를 단순화하려면 이 열을 삭제합니다.

# COMMAND ----------

untagged_df_clean = untagged_df.dropna(axis=1, how="all").copy()
untagged_df_clean.info()

# COMMAND ----------

# MAGIC %md `localHour`의 null 값을`-99`로, `-1`의 값을`-99`로 바꾸어 줍니다.

# COMMAND ----------

untagged_df_clean["localHour"] = untagged_df_clean["localHour"].fillna(-99)
untagged_df_clean.loc[untagged_df_clean.loc[:,"localHour"] == -1, "localHour"] = -99
untagged_df_clean["localHour"].value_counts()

# COMMAND ----------

# MAGIC %md 나머지 널(null) 필드를 정리하십시오.
# MAGIC - 위치 필드의 결 측값을 알 수없는 경우 `NA`로 설정하여 수정합니다.
# MAGIC - `isProxyIP`를 False로 설정
# MAGIC - `cardType`을 알 수없는 경우 `U`로 설정합니다.
# MAGIC - `cvvVerifyResult`를`N`으로 설정하십시오. (잘못된 CVV2 번호가 입력되어 거래가 실패한 경우 CVV2 numebr이 입력되지 않았으므로 CVV2 일치가없는 것처럼 처리합니다.)

# COMMAND ----------

untagged_df_clean = untagged_df_clean.fillna(value={"ipState": "NA", "ipPostcode": "NA", "ipCountryCode": "NA", 
                               "isProxyIP":False, "cardType": "U", 
                               "paymentBillingPostalCode" : "NA", "paymentBillingState":"NA",
                               "paymentBillingCountryCode" : "NA", "cvvVerifyResult": "N"
                              })

# COMMAND ----------

# MAGIC %md `transactionScenario`열은 모든 행이 동일한 `A` 값을 가지므로 정보를 제공하지 않습니다. `transactionType`열도 마찬가지입니다.

# COMMAND ----------

del untagged_df_clean["transactionScenario"]
del untagged_df_clean["transactionType"]

# COMMAND ----------

# MAGIC %md `transactionDateTime`도 이전과 동일한 방법으로 만들어줍니다.

# COMMAND ----------

untagged_df_clean["transactionTime"] = ['{0:06d}'.format(x) for x in untagged_df_clean["transactionTime"]]
untagged_df_clean["transactionDateTime"] = pd.to_datetime(untagged_df_clean["transactionDate"].map(str) + untagged_df_clean["transactionTime"], format='%Y%m%d%H%M%S')
untagged_df_clean["transactionDateTime"]

# COMMAND ----------

# MAGIC %md `untagged_df_clean`은 이제 사용할 준비가 되었습니다.

# COMMAND ----------

# MAGIC %md # Prepare Fraud Transactions

# COMMAND ----------

# MAGIC %md `transactionDeviceId`에는 의미있는 값이 없으므로 삭제하십시오. 또한 태그없는 트랜잭션에 대해했던 것처럼 'localHour'필드의 NA 값을 -99로 채 웁니다

# COMMAND ----------

fraud_df_clean = fraud_df.copy()
del fraud_df_clean['transactionDeviceId']
fraud_df_clean["localHour"] = fraud_df_clean["localHour"].fillna(-99)

# COMMAND ----------

# MAGIC %md 
# MAGIC 그런 다음, untagged 데이터 세트에 사용 된 것과 동일한 방법을 사용하여 transactionDateTime 열을 사기 데이터 세트에 추가하십시오.

# COMMAND ----------

fraud_df_clean["transactionTime"] = ['{0:06d}'.format(x) for x in fraud_df_clean["transactionTime"]]
fraud_df_clean["transactionDateTime"] = pd.to_datetime(fraud_df_clean["transactionDate"].map(str) + fraud_df_clean["transactionTime"], format='%Y%m%d%H%M%S')
fraud_df_clean["transactionDateTime"]

# COMMAND ----------

fraud_df_clean.info()

# COMMAND ----------

# MAGIC %md 
# MAGIC 그런 다음 fraud 데이터 세트에서 중복 행을 제거하십시오. 우리는`transactionID`,`accountID`,`transactionDateTime` 및`transactionAmount`로 고유 한 트랜잭션을 식별합니다.

# COMMAND ----------

fraud_df_clean = fraud_df_clean.drop_duplicates(subset=['transactionID', 'accountID', 'transactionDateTime', 'transactionAmount'], keep='first')

# COMMAND ----------

# MAGIC %md `fraud_df_clean`은 사용할 준비가 되었습니다.

# COMMAND ----------

# MAGIC %md # Enrich the untagged data with account data

# COMMAND ----------

# MAGIC %md 이 섹션에서는 untagged 데이터 세트를 account 데이터 세트와 결합하여 untagged sample을 보강합니다.

# COMMAND ----------

latestTrans_df = pd.merge(untagged_df_clean, account_df_clean, on='accountID', suffixes=('_unt','_act'))

# COMMAND ----------

latestTrans_df

# COMMAND ----------

latestTrans_df = latestTrans_df[latestTrans_df['transactionDateTime_act'] <= latestTrans_df['transactionDateTime_unt']]

# COMMAND ----------

# MAGIC %md 가장 최신 레코드의 timestamp를 찾습니다.

# COMMAND ----------

latestTrans_df = latestTrans_df.groupby(['accountID','transactionDateTime_unt']).agg({'transactionDateTime_act':'max'})

# COMMAND ----------

latestTrans_df

# COMMAND ----------

# MAGIC %md 최근 트랜잭션과 untagged 데이터 프레임, account 데이터 프레임을 조인시킵니다.

# COMMAND ----------

joined_df = pd.merge(untagged_df_clean, latestTrans_df, how='outer', left_on=['accountID','transactionDateTime'], right_on=['accountID','transactionDateTime_unt'])
joined_df

# COMMAND ----------

joined_df = pd.merge(joined_df, account_df_clean, left_on=['accountID','transactionDateTime_act'], right_on=['accountID','transactionDateTime'])
joined_df

# COMMAND ----------

joined_df.info()

# COMMAND ----------

# MAGIC %md 모델에 필요한 열만 선택합니다.

# COMMAND ----------

untagged_join_acct_df = joined_df[['transactionID', 'accountID', 'transactionAmountUSD', 'transactionAmount','transactionCurrencyCode', 'localHour',
          'transactionIPaddress','ipState','ipPostcode','ipCountryCode', 'isProxyIP', 'browserLanguage','paymentInstrumentType',
           'cardType', 'paymentBillingPostalCode', 'paymentBillingState', 'paymentBillingCountryCode', 'cvvVerifyResult',
           'digitalItemCount', 'physicalItemCount', 'accountPostalCode', 'accountState', 'accountCountry', 'accountAge',
           'isUserRegistered', 'paymentInstrumentAgeInAccount', 'numPaymentRejects1dPerUser', 'transactionDateTime_act'
          ]]
untagged_join_acct_df

# COMMAND ----------

# MAGIC %md 이름을 변환하고 접미사를 제거하여 정리합니다.

# COMMAND ----------

untagged_join_acct_df = untagged_join_acct_df.rename(columns={
                                      'transactionDateTime_act':'transactionDateTime'
                                     })

# COMMAND ----------

untagged_join_acct_df.info()

# COMMAND ----------

# MAGIC %md # Labeling fraud examples

# COMMAND ----------

# MAGIC %md 
# MAGIC 먼저 각 계정의 사기 기간을 확인하십시오. 사기 계정 데이터를 `accountID`로 그룹화하면됩니다.

# COMMAND ----------

fraud_t2 = fraud_df_clean.groupby(['accountID']).agg({'transactionDateTime':['min','max']})

# COMMAND ----------

# MAGIC %md 이 새로운 열에 좀 더 친숙한 이름을 지정하십시오.

# COMMAND ----------

fraud_t2.columns = ["_".join(x) for x in fraud_t2.columns.ravel()]

# COMMAND ----------

fraud_t2

# COMMAND ----------

# MAGIC %md 이제 untagged 데이터 세트와 fraud 데이터 세트를 Left Join 합니다.

# COMMAND ----------

untagged_joinedto_ranges = pd.merge(untagged_join_acct_df, fraud_t2, on='accountID', how='left')
untagged_joinedto_ranges

# COMMAND ----------

# MAGIC %md 
# MAGIC 이제 결합 된 데이터를 사용하여 다음 규칙에 따라 레이블을 적용합니다.
# MAGIC * untagged의 accountID는 fraud 데이터 세트에서 0으로 태그되지 않습니다. 이는 사기가 아님을 의미합니다.
# MAGIC * fraud 데이터 세트에서 untagged의 accountID가 발견됬지만, transactionDateTime이 fraud 데이터 세트의 시간 범위를 벗어났고, 2이고 로 태그 된 습니다.
# MAGIC * fraud 데이터 세트에서 untagged의 accountID가 발견됬고, transactionDateTime이 fraud 데이터 세트의 시간 범위 내에 있으며, 사기 태그를 의미하는 1로 태그 되었습니다.

# COMMAND ----------

def label_fraud_range(row):
    if (str(row['transactionDateTime_min']) != "NaT") and (row['transactionDateTime'] >= row['transactionDateTime_min']) and (row['transactionDateTime'] <= row['transactionDateTime_max']):
        return 1
    elif (str(row['transactionDateTime_min']) != "NaT") and row['transactionDateTime'] < row['transactionDateTime_min']:
        return 2
    elif (str(row['transactionDateTime_max']) != "NaT") and row['transactionDateTime'] > row['transactionDateTime_max']:
        return 2
    else:
        return 0

# COMMAND ----------

tagged_df_clean = untagged_joinedto_ranges
tagged_df_clean['label'] = untagged_joinedto_ranges.apply(lambda row: label_fraud_range(row), axis=1)
tagged_df_clean

# COMMAND ----------

# MAGIC %md 이로 인해 1,170 건의 사기 사례, 198,326 건의 사기가 아닌 사례 및 504 건의 사례로 인해 사기 전후에 발생한 것으로 간주 할 수 있습니다.

# COMMAND ----------

tagged_df_clean['label'].value_counts()

# COMMAND ----------

# MAGIC %md 레이블 값이 2 인 예제를 제거하고`transactionDateTime_min` 및`transactionDateTime_max` 을 삭제합니다.

# COMMAND ----------

tagged_df_clean = tagged_df_clean[tagged_df_clean['label'] != 2]
del tagged_df_clean['transactionDateTime_min']
del tagged_df_clean['transactionDateTime_max']

# COMMAND ----------

tagged_df_clean.info()

# COMMAND ----------

# MAGIC %md 파이프 라인에서 사용하기 위해 다음과 같이 변환을 custom transformer로 인코딩합니다.

# COMMAND ----------

import pandas as pd
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

# MAGIC %md 변환 파이프 라인을 테스트 합니다.

# COMMAND ----------

preprocessed_result = preprocessor.fit_transform(tagged_df_clean)

# COMMAND ----------

pd.DataFrame(preprocessed_result).info()

# COMMAND ----------

# MAGIC %md #Train the model

# COMMAND ----------

# MAGIC %md 
# MAGIC 이제 모델을 학습 할 준비가되었습니다. 의사 결정 트리 기반 앙상블 모델 `GradientBoostingClassifier`를 훈련시킵니다.

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier

X = preprocessed_result
y = tagged_df_clean['label']
X_train, X_test, y_train, y_test = train_test_split(X, y)

gbct = GradientBoostingClassifier()
gbct.fit(X_train, y_train)

# COMMAND ----------

# MAGIC %md 이제 훈련 된 모델을 사용하여 테스트 세트에 대해 예측하고 성능을 평가 합니다.

# COMMAND ----------

y_test_preds = gbct.predict(X_test)

# COMMAND ----------

from sklearn.metrics import confusion_matrix, accuracy_score
confusion_matrix(y_test, y_test_preds)

# COMMAND ----------

# MAGIC %md #Test save and load of the model

# COMMAND ----------

# MAGIC %md 
# MAGIC 배치 스코어링시 일반적으로 공유 위치에 저장된 모델로 작업합니다. 이렇게하면 일괄 처리에 모델을 사용하는 작업이 최신 버전의 모델을 쉽게 검색 할 수 있습니다. 모범 사례는 먼저 해당 모델을 Azure Machine Learning 서비스에서 등록하여 버전을 지정하는 것입니다. 그런 다음 모든 작업이 Azure Machine Learning 서비스 레지스트리에서 모델을 검색 할 수 있습니다.
# MAGIC
# MAGIC 다음 셀을 단계별로 진행하여 이를 준비 할 수있는 Helper Function를 만드십시오.

# COMMAND ----------

import os
import azureml
from azureml.core import Workspace
from azureml.core.model import Model
from sklearn.externals import joblib

# COMMAND ----------

def getOrCreateWorkspace(subscription_id, resource_group, workspace_name, workspace_region):
    # By using the exist_ok param, if the worskpace already exists we get a reference to the existing workspace instead of an error
    ws = Workspace.create(
        name = workspace_name,
        subscription_id = subscription_id,
        resource_group = resource_group, 
        location = workspace_region,
        exist_ok = True)
    return ws

# COMMAND ----------

def saveModelToAML(ws, model, model_folder_path="models", model_name="batch-score"):
    # create the models subfolder if it does not exist in the current working directory
    target_dir = './' + model_folder_path
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
  
    # save the model to disk
    joblib.dump(model, model_folder_path + '/' + model_name + '.pkl')
  
    # notice for the model_path, we supply the name of the model outputs folder without a trailing slash
    # anything present in the model folder path will be uploaded to AML along with the model
    print("Registering and uploading model...")
    registered_model = Model.register(model_path=model_folder_path, 
                                      model_name=model_name, 
                                      workspace=ws)
    return registered_model

# COMMAND ----------

def loadModelFromAML(ws, model_name="batch-score"):
  # download the model folder from AML to the current working directory
  model_folder_path = Model.get_model_path(model_name, _workspace=ws)
  model_file_path = os.path.join(model_folder_path, model_name + '.pkl')
  print('Loading model from:', model_file_path)
  model = joblib.load(model_file_path)
  return model

# COMMAND ----------

# MAGIC %md 모델을 Azure Machine Learning 서비스에 저장합니다.

# COMMAND ----------

#Provide the Subscription ID of your existing Azure subscription
subscription_id = #"YOUR_SUBSCRIPTION_ID"

#Provide values for the Resource Group and Workspace that will be created
resource_group = #"YOUR_RESOURCE_GROUP"
workspace_name = "aml-workspace"
workspace_region = 'koreacentral' # eastus, westcentralus, southeastasia, australiaeast, westeurope

#Get an AML Workspace
ws =  getOrCreateWorkspace(subscription_id, resource_group, 
                   workspace_name, workspace_region)

#Save the model to the AML Workspace
registeredModel = saveModelToAML(ws, gbct)

# COMMAND ----------

# MAGIC %md 이제 Azure Machine Learning 서비스에서 모델을 가져 와서 모델을 로드 한 다음 모델을 사용하여 스코어링을 시도해 보십시오.

# COMMAND ----------

# Test loading the model

gbct = loadModelFromAML(ws)
y_test_preds = gbct.predict(X_test)

# COMMAND ----------

y_test_preds

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC 축하합니다, Exercise 3을 완료했습니다. 이번 Workshop에서 나중에 비슷한 논리로 배치 스코어링 작업을 만들것입니다.
# MAGIC
# MAGIC Cosmos DB real-time advanced analytics hands-on lab setup guide로 돌아가서 Exercise 4를 계속합니다.
