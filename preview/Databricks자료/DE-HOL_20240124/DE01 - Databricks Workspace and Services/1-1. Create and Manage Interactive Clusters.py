# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 대화형 클러스터 생성 및 관리
# MAGIC
# MAGIC Databricks 클러스터는 프로덕션 ETL 파이프라인, 스트리밍 분석, 임시 분석 및 기계 학습과 같은 데이터 엔지니어링, 데이터 과학 및 데이터 분석 워크로드를 실행하는 컴퓨트 리소스 및 구성 집합입니다. 이러한 워크로드는 노트북의 명령 세트 또는 자동화된 작업으로 실행됩니다.
# MAGIC
# MAGIC Databricks 클러스터는 다목적(All Purpose) 클러스터와 작업(Job) 클러스터로 구분합니다.
# MAGIC - 다목적 클러스터를 사용하여 대화형 노트북을 사용하여 공동으로 데이터를 분석합니다.
# MAGIC - 작업 클러스터를 사용하여 빠르고 강력한 자동화 작업을 실행합니다.
# MAGIC
# MAGIC 이 데모에서는 Databricks Data Science & Engineering Workspace를 사용하여 다목적(All Purpose) Databricks 클러스터를 만들고 관리하는 방법을 다룹니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 단원을 마치면 다음을 수행할 수 있습니다.
# MAGIC * 클러스터 UI를 사용하여 클러스터 구성 및 배포
# MAGIC * 클러스터 편집, 종료, 재시작 및 삭제

# COMMAND ----------

# MAGIC %md <i18n value="cb2cc2e0-3923-4969-bc01-11698cd1761c"/>
# MAGIC   
# MAGIC ## 클러스터 생성
# MAGIC
# MAGIC 현재 작업 중인 워크스페이스에 적용된 권한에 따라 클러스터 생성 권한이 있을 수도 있고 없을 수도 있습니다.
# MAGIC
# MAGIC 이 단원의 지침은 여러분들이 클러스터 생성 권한이 **있고** 이 과정의 단원을 실행하기 위해 새 클러스터를 배포해야 한다고 가정합니다.
# MAGIC
# MAGIC 단계:
# MAGIC 1. 왼쪽 사이드바에서 ![compute](https://files.training.databricks.com/images/clusters-icon.png) 아이콘을 클릭하여 **Compute** 페이지로 이동합니다.
# MAGIC 1. 파란색 **Create compute** 버튼을 클릭합니다.
# MAGIC 1. 페이지 상단의 **Cluster name**은 본인의 이름을 쉽게 찾을 수 있고 문제가 있을 경우 강사가 쉽게 식별할 수 있는 이름을 사용합니다.
# MAGIC 1. Policy 를 **DBAcademy** 로 선택합니다. 
# MAGIC 1. **Cluster Mode**를 **Single Node**로 설정합니다(Policy 에 의해서 이미 기본으로 선택됨).
# MAGIC 1. **Access Mode** 를 **Single User** 로 선택하고 사용자 명에 본인의 ID 를 선택합니다. 
# MAGIC 1. 이 과정에 권장되는 **Databricks runtime 13.3 version** 사용
# MAGIC 1. **Node Type** 은 **Standard_DS3_v2** 를 선택합니다(Policy에 의해서 이미 선택됨)
# MAGIC 1. 파란색 **Create Cluster** 버튼을 클릭합니다.
# MAGIC
# MAGIC **참고:** 클러스터를 배포하는 데 몇 분 정도 걸릴 수 있습니다. 클러스터 배포를 완료하면 클러스터 생성 UI를 계속 탐색할 수 있습니다.

# COMMAND ----------

# MAGIC %md <i18n value="21e3f0c5-0ced-4742-8f25-07775db546fd"/>
# MAGIC
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> 이 과정에서는 Single-Node Cluster 가 필요합니다
# MAGIC **중요:** 이 과정에서는 단일 노드 클러스터에서 노트북을 실행해야 합니다.
# MAGIC
# MAGIC 위의 지침에 따라 **Cluster Mode**가 **`Single Node`** 로 설정된 클러스터를 생성합니다.

# COMMAND ----------

# MAGIC %md <i18n value="d1a8cf77-f6e8-40df-8355-3a598441457a"/>
# MAGIC
# MAGIC ## 클러스터 관리
# MAGIC
# MAGIC 클러스터가 생성되면 **Compute** 페이지로 돌아가서 클러스터를 살펴봅니다.
# MAGIC
# MAGIC 현재 구성을 검토하려면 생성된 클러스터를 클릭하세요.
# MAGIC
# MAGIC **Edit** 버튼을 클릭합니다. 대부분의 설정은 수정할 수 있습니다(충분한 권한이 있는 경우). 대부분의 설정을 변경하려면 실행 중인 클러스터를 다시 시작해야 합니다.
# MAGIC
# MAGIC **참고**: 다음 레슨에서는 지금 만들어진 클러스터를 사용할 것입니다. 클러스터를 다시 시작, 종료 또는 삭제하면 새로운 리소스가 배포될 때까지 기달려야 할 수 있습니다.

# COMMAND ----------

# MAGIC %md <i18n value="b7ce6ce1-4d68-4a91-b325-3831c1653c67"/>
# MAGIC
# MAGIC ## Restart, Terminate, and Delete
# MAGIC
# MAGIC **Restart**, **Terminate** 및 **Delete**의 효과는 다르지만 모두 클러스터 종료 이벤트를 발생시킵니다. (이 설정을 사용한다고 가정하면 비활성으로 인해 클러스터도 자동으로 종료됩니다.)
# MAGIC
# MAGIC 클러스터가 종료되면 현재 사용 중인 모든 클라우드 리소스가 삭제됩니다. 이는 다음을 의미합니다.
# MAGIC * 연결된 VM 및 메모리가 제거됩니다.
# MAGIC * 연결된 볼륨 저장소가 삭제됩니다.
# MAGIC * 노드 간의 네트워크 연결이 제거됩니다.
# MAGIC
# MAGIC 즉, 컴퓨팅 환경과 이전에 연결된 모든 리소스가 완전히 제거됩니다. 즉, **유지해야 하는 모든 데이터는 영구적인 위치에 저장해야 합니다**. 하지만 여러분의 노트북 코드는 손실되지 않으며 스토리지에 저장한 데이터 파일도 손실되지 않습니다.
# MAGIC
# MAGIC **Restart** 버튼을 사용하면 클러스터를 수동으로 다시 시작할 수 있습니다. 이는 클러스터에서 캐시를 완전히 지워야 하거나 컴퓨팅 환경을 완전히 재설정하려는 경우에 유용할 수 있습니다.
# MAGIC
# MAGIC **Terminate** 버튼을 사용하면 클러스터를 중지할 수 있습니다. 클러스터 구성 설정을 유지하고 **Restart** 버튼을 사용하여 동일한 구성을 사용하는 새로운 클라우드 리소스 집합을 배포할 수 있습니다.
# MAGIC
# MAGIC **Delete** 버튼은 클러스터를 중지하고 클러스터 구성을 제거합니다.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
