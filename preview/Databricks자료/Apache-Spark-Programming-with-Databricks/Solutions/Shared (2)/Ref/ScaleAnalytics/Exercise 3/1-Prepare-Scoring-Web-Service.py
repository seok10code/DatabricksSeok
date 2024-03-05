# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 3 - Task 1: Prepare and deploy scoring web service
# MAGIC
# MAGIC 이 노트북에서는 사기 거래가 발생할 때 이를 탐지하는데 사용되는 모델을 준비합니다. 이 모델은 웹 서비스로 배포됩니다.

# COMMAND ----------

# MAGIC %md # Download data sets

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

# MAGIC %md 
# MAGIC Woodgrove 은행 팀은 모델에 대한 훈련을 위해 내 보낸 데이터의 CSV 사본을 내보냈습니다. 다음 셀을 실행하여 데이터 세트를 다운로드하십시오.

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
# MAGIC

# COMMAND ----------

import os
pathToCsvFile = os.path.join('/dbfs' + tempFolderName, 'Fraud_Transactions.csv')
fraud_df = pd.read_csv(pathToCsvFile)
fraud_df

# COMMAND ----------

pathToCsvFile = os.path.join('/dbfs' + tempFolderName, 'Account_Info.csv')
account_df = pd.read_csv(pathToCsvFile, low_memory=False)
account_df

# COMMAND ----------

pathToCsvFile = os.path.join('/dbfs' + tempFolderName, 'Untagged_Transactions.csv')
untagged_df = pd.read_csv(pathToCsvFile, low_memory=False)
untagged_df

# COMMAND ----------

# MAGIC %md # Prepare Data Sets

# COMMAND ----------

# MAGIC %md 원시(Raw) 데이터에는 다음 셀에서 수행하는 모델 학습에 사용하기 전에 정리해야 할 몇 가지 문제가 있습니다.

# COMMAND ----------

# MAGIC %md ## Prepare Accounts

# COMMAND ----------

# MAGIC %md 
# MAGIC 계정 데이터 세트의 데이터를 정리하여 시작합니다.
# MAGIC `accountOwnerName`,`accountAddress`,`accountCity` 및`accountOpenDate`와 같은 값이 거의 혹은 전혀 없는 열을 제거합니다.

# COMMAND ----------

account_df_clean = account_df[["accountID", "transactionDate", "transactionTime", 
                               "accountPostalCode", "accountState", "accountCountry", 
                               "accountAge", "isUserRegistered", "paymentInstrumentAgeInAccount", 
                               "numPaymentRejects1dPerUser"]]

# COMMAND ----------

# MAGIC %md 데이터 조작이 원본에 영향을 미치지 않도록 데이터 프레임의 복사본을 만듭니다.

# COMMAND ----------

account_df_clean = account_df_clean.copy()

# COMMAND ----------

# MAGIC %md 숫자가 아닌 값 (예: 잘못된 문자열 값 또는 Garbage 데이터)이 NaN으로 변환되었는지 확인한 다음 해당 NaN 값을 0으로 채울 수 있습니다.

# COMMAND ----------

account_df_clean['paymentInstrumentAgeInAccount'] = pd.to_numeric(account_df_clean['paymentInstrumentAgeInAccount'], errors='coerce')
account_df_clean['paymentInstrumentAgeInAccount'] = account_df_clean[['paymentInstrumentAgeInAccount']].fillna(0)['paymentInstrumentAgeInAccount']

# COMMAND ----------

# MAGIC %md 다음으로`numPaymentRejects1dPerUser`를 변환하여 열에`object` 대신`float`의 데이터 유형이 있도록 합니다.

# COMMAND ----------

account_df_clean["numPaymentRejects1dPerUser"] = account_df_clean[["numPaymentRejects1dPerUser"]].astype(float)["numPaymentRejects1dPerUser"]

# COMMAND ----------

# MAGIC %md 이 컬럼을 정리 한 결과를 살펴 보겠습니다. 
# MAGIC 특정 사용자에게 발생하는 지불 거절/거부 대부분이 0 회 또는 1 회 발생한 후에, 하루동안 내에 신속하게 발생 됩니다. 한 유저가 하루에 5 번 거절된 이후의 숫자는 136 개로 줄어듭니다.

# COMMAND ----------

account_df_clean["numPaymentRejects1dPerUser"].value_counts()

# COMMAND ----------

# MAGIC %md `account_df_clean`은 이제 모델링에 사용할 준비가되었습니다.

# COMMAND ----------

# MAGIC %md ## Prepare Untagged Transactions

# COMMAND ----------

# MAGIC %md 
# MAGIC 태그가 지정되지 않은 트랜잭션 데이터 세트를 정리하십시오. untagged_transactions에는 값이 모두 null 인 열이 16 개 있습니다.이 열을 삭제하여 데이터 세트를 단순화하겠습니다.

# COMMAND ----------

untagged_df_clean = untagged_df.dropna(axis=1, how="all").copy()

# COMMAND ----------

# MAGIC %md 널이 아닌 값의 수를 검사하고 다음 셀을 실행하여 각 열의 유추 된 데이터 유형을 볼 수 있습니다. 셀의 출력을 살펴보면 다음 할 일이 보입니다. 시작을 위해 null이 아닌 값이 200,000 미만인 열이 있습니다. 이는 해당 열에 수정해야 할 일부 null 값이 있음을 의미합니다.

# COMMAND ----------

untagged_df_clean.info()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC localHour 필드를 정리해 봅시다. `localHour`의 null 값을`-99`로, `-1`의 값을`-99`로 바꿉니다.

# COMMAND ----------

untagged_df_clean["localHour"] = untagged_df_clean["localHour"].fillna(-99)
untagged_df_clean.loc[untagged_df_clean.loc[:,"localHour"] == -1, "localHour"] = -99

# COMMAND ----------

# MAGIC %md 이제 값이 좋아 보이는지 확인하십시오.

# COMMAND ----------

untagged_df_clean["localHour"].value_counts()

# COMMAND ----------

# MAGIC %md 
# MAGIC 나머지 널(null) 필드를 정리하십시오.
# MAGIC - 위치 필드의 결 측값을 알 수없는 경우 `NA`로 설정하여 수정합니다.
# MAGIC - `isProxyIP`를 False로 설정
# MAGIC - `cardType`을 알 수없는 경우 `U`로 설정합니다.
# MAGIC - `cvvVerifyResult`를`N`으로 설정하십시오. (잘못된 CVV2 번호가 입력되어 거래가 실패한 경우 CVV2 numebr이 입력되지 않았으므로 CVV2 일치가없는 것처럼 처리합니다.)
# MAGIC

# COMMAND ----------

untagged_df_clean = untagged_df_clean.fillna(value={"ipState": "NA", "ipPostcode": "NA", "ipCountryCode": "NA", 
                               "isProxyIP":False, "cardType": "U", 
                               "paymentBillingPostalCode" : "NA", "paymentBillingState":"NA",
                               "paymentBillingCountryCode" : "NA", "cvvVerifyResult": "N"
                              })

# COMMAND ----------

# MAGIC %md 모든 null 값이 해결되었는지 확인합니다.

# COMMAND ----------

untagged_df_clean.info()

# COMMAND ----------

# MAGIC %md 
# MAGIC `transactionScenario`열은 모든 행이 동일한 `A` 값을 가지므로 정보를 제공하지 않습니다. `transactionType`열도 마찬가지입니다.

# COMMAND ----------

del untagged_df_clean["transactionScenario"]

# COMMAND ----------

del untagged_df_clean["transactionType"]

# COMMAND ----------

# MAGIC %md `untagged_df_clean`은 이제 사용할 준비가 되었습니다.

# COMMAND ----------

# MAGIC %md ## Prepare Fraud Transactions

# COMMAND ----------

# MAGIC %md 이제 사기 거래 데이터 세트 준비로 넘어갑니다.
# MAGIC
# MAGIC `transactionDeviceId`는 의미있는 값을 가지지 않으므로 삭제합니다.

# COMMAND ----------

fraud_df_clean = fraud_df.copy()
del fraud_df_clean['transactionDeviceId']

# COMMAND ----------

# MAGIC %md 사기 데이터 세트에는 계정 데이터 세트에서와 같이 누락 된 값을 채워야하는 localHour 필드가 있습니다.

# COMMAND ----------

fraud_df_clean["localHour"] = fraud_df_clean["localHour"].fillna(-99)

# COMMAND ----------

# MAGIC %md 작업을 내용을 검사해보면, 각 열에 널이 아닌 값이 8640이어야합니다.

# COMMAND ----------

fraud_df_clean.info()

# COMMAND ----------

# MAGIC %md `fraud_df_clean`은 이제 준비가 되었습니다.

# COMMAND ----------

# MAGIC %md ## Create labels

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 목표는 모든 거래가 포함 된 데이터 프레임을 생성하는 것입니다. 각 거래는`isFraud` 열을 통해`0`-사기 없음 또는`1`-사기 값으로 태그됩니다.
# MAGIC
# MAGIC fraud 데이터 프레임에도 나타나는 untagged_transactions 데이터 프레임에 나타나는 모든 거래는 사기로 표시됩니다.
# MAGIC
# MAGIC 나머지 거래는 사기가 아닌 것으로 표시됩니다.
# MAGIC
# MAGIC 다음 셀을 실행하여 레이블 시리즈(Labels Series)를 작성하십시오.

# COMMAND ----------

all_labels = untagged_df_clean["transactionID"].isin(fraud_df_clean["transactionID"])

# COMMAND ----------

all_transactions = untagged_df_clean

# COMMAND ----------

# MAGIC %md ## Create Feature Engineering Pipeline

# COMMAND ----------

# MAGIC %md 
# MAGIC 다음 셀에서는 파이프 라인에서 데이터를 준비하는 데 사용할 두 개의 사용자 지정 pipeline을 정의합니다.
# MAGIC
# MAGIC 이러한 pipeline을 모듈로 수집 한 다음, 이 모듈 파일을 models 디렉토리에 저장합니다. 그런 다음 모델 학습 및 모델 스코어링 중에 이 모듈의 클래스를 사용합니다. 배포 중에 Model.register를 사용하여 모델을 등록하면 (나중에`deployModelAsWebService`에서와 같이) models 디렉토리의 모든 파일이 모델과 함께 업로드됩니다.
# MAGIC
# MAGIC 스코어링 전에 데이터를 변환하는 데 사용되는 pipeline이 포함 된 모듈을 모델과 함께 배치해야합니다. 이를 통해 직렬화 된 파이프라인이 로드되어 스코어링에 사용될 때 웹 서비스 컨텍스트에서 코드를 실행할 수 있습니다.
# MAGIC
# MAGIC 먼저 models 디렉토리가 없다면 작성해야합니다.

# COMMAND ----------

import os
models_dir = tempFolderName + "/models/"
if not os.path.exists(models_dir):
    os.makedirs(models_dir)

# COMMAND ----------

# MAGIC %md 그런 다음 이 esitimators 모듈을 저장할 수 있습니다.

# COMMAND ----------

# write out to models/customestimators.py
scoring_service = """
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
        return X
""" 

with open(models_dir + "customestimators.py", "w") as file:
    file.write(scoring_service)

# COMMAND ----------

# MAGIC %md 이 노트북 환경에서 모듈을 찾고 가져올 수 있도록 작성된 모듈을 Python 검색 경로에 추가해야합니다.

# COMMAND ----------

import sys
from os.path import dirname
sys.path.append(models_dir)

# COMMAND ----------

# MAGIC %md 다음에, estimators를 가져옵니다.

# COMMAND ----------

from customestimators import NumericCleaner, CategoricalCleaner

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 이제 데이터를 준비 할 파이프라인을 구축하십시오.
# MAGIC
# MAGIC 다음 셀의 요점은 데이터 준비를 두 경로로 분할하여 데이터 세트를 세로로 분할 한 다음 결과를 결합하는 것입니다. `ColumnTransformer`는 숫자 변환의 결과 데이터 프레임과 범주 변환의 결과 데이터 프레임을 효과적으로 연결합니다.
# MAGIC
# MAGIC - 숫자 변환기 파이프 라인 : 이전에 생성 된 사용자 정의 변환기를 사용하여 숫자 열을 정리합니다. 학습 할 모델은 Support Vector Machine 분류기(classifier)이므로 `StandardScaler`가 제공하는 숫자 값의 규모를 표준화 해야 합니다.
# MAGIC - 범주 형 변압기 파이프 라인 : 이전에 분류 열을 정리 한 사용자 정의 변압기를 사용합니다. 그런 다음 각 범주 형 열의 각 값을 단일 핫 인코딩하여 각 가능한 값에 대해 하나의 열이있는 더 넓은 데이터 프레임을 만듭니다 (1은 해당 값이있는 행에 나타남).

# COMMAND ----------

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

numeric_features=["transactionAmountUSD", "transactionDate", "transactionTime", "localHour", 
                  "transactionIPaddress", "digitalItemCount", "physicalItemCount"]

categorical_features=["transactionCurrencyCode", "browserLanguage", "paymentInstrumentType", "cardType", "cvvVerifyResult"]                           

numeric_transformer = Pipeline(steps=[
    ('cleaner', NumericCleaner()),
    ('scaler', StandardScaler())
])
                               
categorical_transformer = Pipeline(steps=[
    ('cleaner', CategoricalCleaner()),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# COMMAND ----------

import sklearn
print(sklearn.__version__)

# COMMAND ----------

# MAGIC %md 이 변환 파이프 라인을 통해 모든 히스토리 데이터를 실행하고 결과 모양을 관찰하십시오.

# COMMAND ----------

preprocessed_result = preprocessor.fit_transform(all_transactions)

# COMMAND ----------

preprocessed_result.shape

# COMMAND ----------

pd.DataFrame(preprocessed_result.todense())

# COMMAND ----------

# MAGIC %md # Create Pipeline and Train a simple model

# COMMAND ----------

# MAGIC %md 이제 이전에 만든 변환 파이프 라인을 기반으로 사기성 또는 사기성 아님으로 분류하는 모델을 학습시킵니다.
# MAGIC
# MAGIC 다음 셀을 실행하여 파이프 라인의 종속성(Dependency)을 가져 왔는지 확인하십시오 (아마 이미 가지고 있지만 여기에 명확하게 로드하면 코드를 웹 서비스로 이식 할 때 도움이 됩니다).

# COMMAND ----------

from customestimators import NumericCleaner, CategoricalCleaner
from sklearn.model_selection import train_test_split

# COMMAND ----------

# MAGIC %md 명백하게도, 우리의 데이터에는 사기성이 아닌 많은 샘플이 있습니다. 모델 학습을 계속하면 부정을 예측할 수 있도록 모델을 효과적으로 학습 할 수 있습니다. 한 클래스(사기아님)가 다른 클래스(사기)보다 훨씬 더 자주 나타나는 상황을 클래스 불균형이라고하며, 그 영향을 완화하기 위해 우리는 비 사기 샘플의 수를 줄여서 동일한 수의 사기 및 사기 샘플을 만듭니다.
# MAGIC
# MAGIC 다음 셀을 실행하여 크기를 줄인 다음 사기가 아닌 1,151 개의 행을 무작위로 샘플링 한 다음이 행을 1,151 개의 사기 행과 통합합니다.

# COMMAND ----------

only_fraud_samples = all_transactions.loc[all_labels == True]
only_fraud_samples["label"] = True
only_non_fraud_samples = all_transactions.loc[all_labels == False]
only_non_fraud_samples["label"] = False
random_non_fraud_samples = only_non_fraud_samples.sample(n=1151, replace=False, random_state=42)
balanced_transactions = random_non_fraud_samples.append(only_fraud_samples)

balanced_transactions["label"].value_counts()

# COMMAND ----------

# MAGIC %md 다음으로, 레이블이 입력 기능으로 사용되지 않도록 레이블 열을 데이터 프레임에서 분리해야합니다.

# COMMAND ----------

balanced_labels = balanced_transactions["label"]
del balanced_transactions["label"]

# COMMAND ----------

# MAGIC %md 
# MAGIC 이제 학습 데이터 프레임의 서브 세트를 작성합니다. 하나는 모델`X_train` 및`y_train`을 훈련하는 데 사용되고 다른 하나는 성능`X_test` 및`y_test`를 테스트하기 위해 남겨 둡니다.

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(balanced_transactions, balanced_labels, 
                                                    test_size=0.2, random_state=42)

# COMMAND ----------

# MAGIC %md 이제 모델을 훈련 시키십니다. 이 경우`LinearSVC` 클래스를 사용합니다.

# COMMAND ----------

import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import LinearSVC

svm_clf = Pipeline((
    ("preprocess", preprocessor),
    ("linear_svc", LinearSVC(C=1, loss="hinge"))
))
svm_clf.fit(X_train, y_train)

# COMMAND ----------

# MAGIC %md 테스트 세트에서 단일 행에 대해 예측하는 모델을 테스트하십시오.

# COMMAND ----------

svm_clf.predict(X_test[0:1])

# COMMAND ----------

# MAGIC %md 다음으로 훈련 세트의 모든 데이터에 대해 얼마나 잘 예측되는지 검토하여 모델을 평가하십시오.

# COMMAND ----------

y_train_preds = svm_clf.predict(X_train)

# COMMAND ----------

# MAGIC %md 
# MAGIC 혼돈행렬(Confusion Matrix)을 사용하여 사기 및 사기 (왼쪽 및 오른쪽 아래 값)를 정확하게 예측할 때 모델의 성능을 확인하십시오. 또한 모델의 실수(왼쪽 하단 및 오른쪽 상단 값)를 조사하십시오. 아래에서, 열 헤더는 사기가 아닌 것으로 예측되고 사기이며, 행 헤더는 실제로 사기가 아닌 것으로서 실제로 사기입니다 (예를 들어, 훈련 데이터에 의해 기술 된 바와 같이).

# COMMAND ----------

from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
confusion_matrix(y_train, y_train_preds)

# COMMAND ----------

# MAGIC %md 분류모델에 대한 공통 메트릭 세트를 사용하여 모델의 성능을 살펴보십시오.

# COMMAND ----------

print("Accuracy:", accuracy_score(y_train, y_train_preds))
print("Precision:", precision_score(y_train, y_train_preds))
print("Recall:", recall_score(y_train, y_train_preds))
print("F1:", f1_score(y_train, y_train_preds))
print("AUC:", roc_auc_score(y_train, y_train_preds))

# COMMAND ----------

# MAGIC %md 
# MAGIC 이 모델은 단지 parsimonous 모델이므로,이 모델은 무작위보다 성능이 좋은 시작을 제공합니다 (AUC가 0.5보다 큰 것으로 표시됨). 프로덕션에 배포하기 전에 수행하려는 현재 성능 이상의 기능을 향상시키기 위해 추가 기능 엔지니어링과 같은 추가 작업이 수행 될 수 있지만이 실습에서는 다루지 않습니다. parsiminous 모델은 데이터가 주어지면 원하는 분류가 가능한지 여부를 확인하고 서비스로 배포하여 조기에 통합 할 수있는 대상에 빠르게 도달 할 수 있도록합니다. 그런 다음 개선 된 버전의 모델 배포를 반복 할 수 있습니다.

# COMMAND ----------

# MAGIC %md 이제 훈련 된 모델이 보지 못한 데이터를 사용하여 테스트 데이터 세트를 사용하여 동일하게 평가하십시오.

# COMMAND ----------

y_test_preds = svm_clf.predict(X_test)
print(confusion_matrix(y_test, y_test_preds))
print(accuracy_score(y_test, y_test_preds))
print("Accuracy:", accuracy_score(y_test, y_test_preds))
print("Precision:", precision_score(y_test, y_test_preds))
print("Recall:", recall_score(y_test, y_test_preds))
print("F1:", f1_score(y_test, y_test_preds))
print("AUC:", roc_auc_score(y_test, y_test_preds))

# COMMAND ----------

# MAGIC %md 보지 않은 데이터 (테스트 데이터)에 대한 모델의 전반적인 성능은 훈련 데이터로 수행하는 방법과 유사합니다. 모델이 훈련 데이터에 과적합(Overfitting)하지 않았다는 것을 나타내는 좋은 신호입니다.
# MAGIC
# MAGIC 다음으로 웹 서비스로 배포 할 모델을 준비하는 단계를 살펴 보겠습니다.

# COMMAND ----------

# MAGIC %md # Save the model to disk

# COMMAND ----------

# MAGIC %md 모델을 배포하기 위해 모델을 디스크에 저장해야합니다.

# COMMAND ----------

from sklearn.externals import joblib
joblib.dump(svm_clf, models_dir + 'fraud_score.pkl')

# COMMAND ----------

# MAGIC %md # Test loading the model

# COMMAND ----------

# MAGIC %md 다음으로, 웹 서비스가 수행해야하는 것처럼 디스크에서 모델을 다시 로드하는 것을 시뮬레이션하십시오.

# COMMAND ----------

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from customestimators import NumericCleaner, CategoricalCleaner
from sklearn.externals import joblib

scoring_pipeline = joblib.load(models_dir + 'fraud_score.pkl')

# COMMAND ----------

pathToCsvFile = os.path.join('/dbfs' + tempFolderName, 'Untagged_Transactions.csv')
untagged_df_fresh = pd.read_csv(pathToCsvFile, low_memory=False)

test_pipeline_preds = scoring_pipeline.predict(untagged_df_fresh)
test_pipeline_preds

# COMMAND ----------

one_row = untagged_df_fresh.iloc[:1]
test_pipeline_preds2 = scoring_pipeline.predict(one_row)
test_pipeline_preds2

# COMMAND ----------

# MAGIC %md # Deploy Model

# COMMAND ----------

# MAGIC %md 이제 Azure Machine Learning 서비스 SDK를 사용하여 프로그래밍 방식으로 작업 영역을 만들고, 모델을 등록하고,이를 사용하는 웹 서비스에 대한 컨테이너 이미지를 만들고 해당 이미지를 Azure 컨테이너 인스턴스에 배포합니다.
# MAGIC
# MAGIC 다음 셀을 실행하여 배치에 사용할 일부 Helper Function을 작성하십시오.

# COMMAND ----------

import azureml
from azureml.core import Workspace
from azureml.core.model import Model

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

def deployModelAsWebService(ws, model_folder_path="models", model_name="score", 
                scoring_script_filename="score.py", 
                conda_packages=['scikit-learn==0.21.1','numpy','pandas'],
                conda_file="dependencies.yml", runtime="python",
                cpu_cores=1, memory_gb=1, tags={'name':'scoring'},
                description='Scoring web service.',
                service_name = "scoringservice"
               ):
    # notice for the model_path, we supply the name of the outputs folder without a trailing slash
    # this will ensure both the model and the customestimators get uploaded.
    print("Registering and uploading model...")
    registered_model = Model.register(model_path=model_folder_path, 
                                      model_name=model_name, 
                                      workspace=ws)

    # create a Conda dependencies environment file
    print("Creating conda dependencies file locally...")
    from azureml.core.conda_dependencies import CondaDependencies 
    mycondaenv = CondaDependencies.create(conda_packages=conda_packages)
    #dbutils.fs.put(tempFolderName + "/" + conda_file, mycondaenv.serialize_to_string(), overwrite=True)
    with open(conda_file,"w") as f:
        f.write(mycondaenv.serialize_to_string())
    
    # create inference configuration
    print("Creating inference configuration...")
    from azureml.core.model import InferenceConfig
    inference_config = InferenceConfig(runtime=runtime, 
                                       entry_script=scoring_script_filename,
                                       conda_file=conda_file)
    
    # create ACI configuration
    print("Creating ACI configuration...")
    from azureml.core.webservice import AciWebservice, Webservice
    aci_config = AciWebservice.deploy_configuration(
        cpu_cores = cpu_cores, 
        memory_gb = memory_gb, 
        tags = tags, 
        description = description)

    # deploy the webservice to ACI
    print("Deploying webservice to ACI...")
    webservice = Model.deploy(workspace=ws,
                              name=service_name,
                              models=[registered_model],
                              inference_config=inference_config,
                              deployment_config=aci_config)
    webservice.wait_for_deployment(show_output=True)
    
    return webservice

# COMMAND ----------

# MAGIC %md 
# MAGIC 다음 셀에서 배포하려는 Azure 구독의`subscription_id`를 제공하십시오. 또한 작업 공간을 보유하기 위해 해당 등록에서 작성하려는 자원 그룹의 이름을 `resource_group`변수에 제공하십시오. 
# MAGIC 선택적으로 기본 작업 공간 이름 및 지역을 변경할 수 있습니다.
# MAGIC
# MAGIC Azure Portal의 Azure Databricks 작업 영역 개요 블레이드에서 Azure 구독 ID를 찾을 수 있습니다.

# COMMAND ----------

#Provide the Subscription ID of your existing Azure subscription
subscription_id = #"YOUR_SUBSCRIPTION_ID"

#Provide values for the Resource Group and Workspace that will be created
resource_group = #"YOUR_RESOURCE_GROUP"
workspace_name = "aml-workspace"
workspace_region = 'koreacentral' # eastus, westcentralus, southeastasia, australiaeast, westeurope

# COMMAND ----------

# MAGIC %md 모델을 로드하고 스코어링에 사용하는 웹 서비스는 Azure Machine Learning 서비스 SDK가 파일을 배포하기 위해 파일에 저장해야합니다. 이 파일을 작성하려면 다음 셀을 실행하십시오.

# COMMAND ----------

# write out the file scoring-service.py
scoring_service = """
import sys
import os
import json
import numpy
from sklearn.externals import joblib
from azureml.core.model import Model
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin


def init():
    global model
    
    try:
        model_path = Model.get_model_path('fraud-score')
        
        print("Loading customestimators module...")
        sys.path.append(model_path)
        from customestimators import NumericCleaner, CategoricalCleaner
        print("Finished loading customestimators module")
        
        model_file_path = os.path.join(model_path,'fraud_score.pkl')
        print('Loading model from:', model_file_path)
        model = joblib.load(model_file_path)
    except Exception as e:
        print(e)
        
# note you can pass in multiple rows for scoring
def run(raw_data):
    try:
        print("Received input:", raw_data)
        input_df = pd.read_json(raw_data, orient='table')
        result = model.predict(input_df)
        # you can return any datatype as long as it is JSON-serializable
        return result.tolist()
    except Exception as e:
        error = str(e)
        return error
"""

with open(tempFolderName + "/" + "score.py", "w") as file:
    file.write(scoring_service)

# COMMAND ----------

# MAGIC %md 
# MAGIC 작업 공간을 작성하거나 이미 존재하는 경우 기존 작업 공간을 검색하여 모델을 배치하십시오.
# MAGIC
# MAGIC 다음 몇 개의 셀을 실행하는 데 ** 7 **에서 ** 10 ** 분이 소요될 수 있습니다.
# MAGIC
# MAGIC ** 중요참고 ** : 셀 아래에 출력되는 텍스트로 로그인하라는 메시지가 표시됩니다. 표시된 URL로 이동하여 제공된 코드를 입력하십시오. 코드를 입력 한 후이 노트북으로 돌아가서 출력에 `Deployed Workspace with name ....`이 표시 될 때까지 기다리세요.

# COMMAND ----------

ws =  getOrCreateWorkspace(subscription_id, resource_group, 
                   workspace_name, workspace_region)


# COMMAND ----------

# It is important to change the current working directory so that your generated scoring-service.py is at the root of it. 
# This is required by the Azure Machine Learning SDK
os.chdir(tempFolderName)
os.getcwd()

# COMMAND ----------

webservice = deployModelAsWebService(ws, model_name="fraud-score")

# COMMAND ----------

# MAGIC %md 마지막으로 배포 된 웹 서비스를 테스트 합니다.

# COMMAND ----------

# test the web service
import json
pathToCsvFile = os.path.join('/dbfs' + tempFolderName, 'Untagged_Transactions.csv')
untagged_df_fresh = pd.read_csv(pathToCsvFile, low_memory=False)
input_df = untagged_df_fresh.iloc[:5]
json_df = input_df.to_json(orient='table')
result = webservice.run(input_data=json_df)
result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC 사기 탐지 모델을 교육, 배포 및 테스트 했으므로 이제 배치 점수 매기기에 사용될 의심스러운 활동을 탐지하는 데 사용되는 모델을 준비하는 작업으로 넘어갑니다.
# MAGIC [Exercise 3 - Task 2: Prepare batch scoring model]($./2-Prepare-Batch-Scoring-Model) notebook으로 계속 진행합니다.
