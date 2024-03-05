# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="36722caa-e827-436b-8c45-3e85619fd2d0"/>
# MAGIC
# MAGIC
# MAGIC # Databricks Workflows 작업 오케스트레이션
# MAGIC
# MAGIC Databricks 작업 UI에 대한 새로운 업데이트에는 작업의 일부로 여러 작업을 예약하는 기능이 추가되어 Databricks 작업이 대부분의 프로덕션 워크로드에 대한 오케스트레이션을 완전히 처리할 수 있습니다.
# MAGIC
# MAGIC 여기서는 노트북 작업을 트리거된 독립 실행형 작업으로 예약하는 단계를 통해서 데이터파이프라인을 생성하는 것을 실습합니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 단원을 마치면 다음을 수행할 수 있습니다.
# MAGIC * Databricks 워크플로 작업에서 노트북 작업 예약
# MAGIC * 클러스터 유형 간의 작업 예약 옵션 및 차이점 설명
# MAGIC * 작업 실행을 검토하여 진행 상황을 추적하고 결과를 확인합니다.
# MAGIC * Databricks Workflows UI를 사용하여 작업 간의 종속성 구성

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %md <i18n value="71b010a3-80be-4909-9b44-6f68029f16c0"/>
# MAGIC
# MAGIC ## Job 생성 및 구성
# MAGIC
# MAGIC 단계:
# MAGIC 1. 왼쪽 사이드바에서 **Workflows** <img src="https://docs.databricks.com/_images/jobs-icon.png"> 버튼을 클릭하고 **Delta Live Tables** 탭을 클릭한 다음 **Create Job** <img src="https://docs.databricks.com/_images/create-job.png">을 클릭합니다.
# MAGIC 2. 왼쪽 상단의 "Add a name for yout job"으로 되어 있는 부분에 Job 이름을 변경합니다. (예: 사용자ID_JOB) 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze Task (sales_orders_raw) 생성
# MAGIC 1. 아래 지정된 첫 번째 Task를 구성합니다.
# MAGIC
# MAGIC    
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | sales_orders_raw |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task1-1 - Bronze sales_orders_raw 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Parameters | **+ Add** 클릭 후 Key에 **`datasets_path`**, Value 에 **`dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02`** |
# MAGIC
# MAGIC
# MAGIC 2. **Create** 클릭.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze Task (customers) 생성
# MAGIC 1. 아래 지정된 Task를 구성합니다.
# MAGIC 2. **Add Task** 버튼 클릭
# MAGIC 3. 팝업 창에서 Notebook 선택
# MAGIC
# MAGIC    
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | customers |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task1-1 - Bronze customers 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Parameters | **+ Add** 클릭 후 Key에 **`datasets_path`**, Value 에 **`dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02`** |
# MAGIC | Depends on | 아무것도 선택하지 않습니다 |
# MAGIC
# MAGIC 4. **Create task** 클릭.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver Task (sales_orders_cleaned) 생성
# MAGIC 1. 아래 지정된 Task를 구성합니다.
# MAGIC 2. **Add Task** 버튼 클릭
# MAGIC 3. 팝업 창에서 Notebook 선택
# MAGIC
# MAGIC    
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | sales_orders_cleaned |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task2 - Silver sales_orders_cleaned 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Depends on | `salse_orders_raw` , `customers` 선택 |
# MAGIC
# MAGIC
# MAGIC 4. **Create task** 클릭.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gold Task (LA_Summary) 생성
# MAGIC 1. 아래 지정된 Task를 구성합니다.
# MAGIC 2. **Add Task** 버튼 클릭
# MAGIC 3. 팝업 창에서 Notebook 선택
# MAGIC
# MAGIC    
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | LA_Summary |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task3 - Gold LA summary 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Depends on | `sales_orders_cleaned` 선택 |
# MAGIC
# MAGIC
# MAGIC 4. **Create task** 클릭.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gold Task (TX_Summary) 생성
# MAGIC 1. 아래 지정된 Task를 구성합니다.
# MAGIC 2. **Add Task** 버튼 클릭
# MAGIC 3. 팝업 창에서 Notebook 선택
# MAGIC
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | TX_Summary |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task3 - Gold TX summary 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Depends on | `sales_orders_cleaned` 선택 |
# MAGIC
# MAGIC
# MAGIC 4. **Create task** 클릭.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Gold Task (Top_10) 생성
# MAGIC 1. 아래 지정된 Task를 구성합니다.
# MAGIC 2. **Add Task** 버튼 클릭
# MAGIC 3. 팝업 창에서 Notebook 선택
# MAGIC
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Top_10 |
# MAGIC | Type |  **Notebook** 선택 |
# MAGIC | Source | **Workspace** 선택|
# MAGIC | Path | Task3 - Gold Top 10 선택 |
# MAGIC | Cluster | 이전 실습에서 생성된 All Purpose 클러스터 선택 |
# MAGIC | Depends on | `sales_orders_cleaned` 선택 |
# MAGIC
# MAGIC
# MAGIC 4. **Create task** 클릭.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## JOB 실행
# MAGIC
# MAGIC 오른쪽 상단의 <img src="https://docs.databricks.com/_images/run-now-button.png"> 버튼을 클릭해서 생성한 Job을 실행 합니다. 

# COMMAND ----------

# MAGIC %md <i18n value="50665a01-dd6c-4767-b8ef-56ee02dbd9db"/>
# MAGIC
# MAGIC ## Review Run
# MAGIC
# MAGIC
# MAGIC 작업 실행을 검토하려면
# MAGIC 1. 화면 왼쪽 상단에서 **Runs** 탭을 선택합니다(현재 **Tasks** 탭에 있어야 함).
# MAGIC 1. **Start time** 컬럼 아래의 타임스탬프 필드를 클릭하여 세부 정보를 엽니다.
# MAGIC 1. **작업이 계속 실행 중인 경우** **`Succeeded`** 또는 **`Pending`** 의 **Status** 와 함께 노트북의 활성 상태가 표시됩니다. . **작업이 완료된** 경우  **`Succeeded`** 또는 **`Failed`** 의 **Status** 와 함께 노트북의 전체 실행이 표시됩니다.
# MAGIC 1. 수행이 완료된 Top_10 Task 를 클릭해서 수행된 노트북의 결과를 리뷰합니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Databricks Job 의 Scheduler 예약
# MAGIC
# MAGIC Jobs UI 오른쪽의 **Job Details** 섹션 바로 아래에 **Schedule** 섹션이 있습니다.
# MAGIC
# MAGIC 일정 옵션을 탐색하려면 **Add schedule** 버튼을 선택합니다.
# MAGIC
# MAGIC **Trigger type**을 **None (Manual)** 에서 **Scheduled** 으로 변경하면 크론 예약 UI가 나타납니다.
# MAGIC
# MAGIC 이 UI는 작업의 시간순 일정을 설정하기 위한 광범위한 옵션을 제공합니다. UI로 구성된 설정은 cron 구문으로 출력될 수도 있으며, UI로 사용할 수 없는 사용자 지정 구성이 필요한 경우 편집할 수 있습니다.
# MAGIC
# MAGIC 지금은 작업을 **Manual** 예약으로 설정해 둡니다. 작업 세부 정보로 돌아가려면 **Cancel**를 선택하세요.

# COMMAND ----------

# MAGIC %md <i18n value="4fecba69-f1cf-4413-8bc6-7b50d32b2456"/>
# MAGIC
# MAGIC ## 다중 작업 실행 결과 검토
# MAGIC
# MAGIC 상단의 **Runs** 탭을 다시 선택한 다음 작업 완료가 화면 아래 테이블에 표 형태로 표시됩니다
# MAGIC
# MAGIC 작업에 대한 시각화는 실시간으로 업데이트되어 현재 실행 중인 작업을 반영하고 작업 실패가 발생하면 색상이 변경됩니다.
# MAGIC
# MAGIC 태스크 상자를 클릭하면 UI에 예약된 노트북이 렌더링됩니다.
# MAGIC
# MAGIC 이는 이전 Databricks 작업 UI 위에 추가된 오케스트레이션 계층으로 생각할 수 있습니다. CLI 또는 REST API로 작업을 예약하는 워크로드가 있는 경우 <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">JSON 작업에 대한 결과를 구성하고 가져오는 데 사용되는 데이터 구조는 UI와 유사합니다.</a>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
