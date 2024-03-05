# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Databricks Notebook Quickstart
# MAGIC 데이터브릭스는 Apache Spark™의 제작자가 만든 **통합 분석 플랫폼**으로 데이터 준비, 탐색 및 분석과 머신러닝 어플리케이션의 디플로이까지 전체적인 머신러닝/AI 라이프사이클 구성을 하는데 데이터 엔지니어, 데이터 과학자, 분석가들이 같이 협업할 수 있는 공간을 제공합니다.  
# MAGIC   
# MAGIC <br>  
# MAGIC   
# MAGIC
# MAGIC <img width="1098" alt="image" src="https://user-images.githubusercontent.com/91228557/168506128-ce86ce8c-5ec3-4a8c-8157-3102a26a37d5.png">
# MAGIC
# MAGIC
# MAGIC 노트북은 데이터브릭스에서 코드를 개발하고 수행하는 가장 기본적인 톨입니다. 이 데이터브릭스의 노트북 환경을 통해 다양한 업무들을 협업하는 과정을 알아봅시다!

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## 1. 클러스터에 노트북 붙이기
# MAGIC
# MAGIC 이전 레슨에서 클러스터를 이미 배포했거나 관리자가 사용하도록 구성한 클러스터를 식별했어야 합니다.
# MAGIC
# MAGIC 화면 상단의 이 노트북 이름 옆에 있는 드롭다운 목록을 사용하여 이 노트북을 클러스터에 연결합니다.
# MAGIC
# MAGIC **참고**: 클러스터를 배포하는 데 몇 분 정도 걸릴 수 있습니다. 리소스가 배포되면 클러스터 이름 오른쪽에 녹색 화살표가 나타납니다. 클러스터 왼쪽에 단색 회색 원이 있는 경우 다음 지침을 따라야 합니다.<br> 
# MAGIC <a href="$./DE 1.1 - Create and Manage Interactive Clusters"> 클러스터 시작하기</a>.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2.노트북 기초
# MAGIC
# MAGIC 노트북은 코드의 셀 단위 실행을 제공합니다. 노트북에서 여러 언어를 혼합할 수 있습니다. 사용자는 플롯, 이미지 및 마크다운 텍스트를 추가하여 코드를 향상시킬 수 있습니다.
# MAGIC
# MAGIC 노트북은 학습 도구로 설계되었습니다. 노트북은 Databricks를 사용하여 프로덕션 코드로 쉽게 배포할 수 있을 뿐만 아니라 데이터 탐색, 보고 및 대시보드 작성을 위한 강력한 도구 집합을 제공할 수 있습니다.
# MAGIC
# MAGIC ### Cell 실행하기
# MAGIC * 다음 옵션 중 하나를 사용하여 아래 셀을 실행합니다.
# MAGIC    * **CTRL+ENTER** 또는 **CTRL+RETURN**
# MAGIC    * **SHIFT+ENTER** 또는 **SHIFT+RETURN** 셀을 실행하고 다음 셀로 이동
# MAGIC    * 여기에 표시된 대로 **Run Cell**, **Run All Above** 또는 **Run All Below** 사용<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### 기본 노트북 언어 설정
# MAGIC
# MAGIC 노트북의 현재 기본 언어가 Python으로 설정되어 있기 때문에 위의 셀은 Python 명령을 실행합니다.
# MAGIC
# MAGIC Databricks 노트북은 Python, SQL, Scala 및 R을 지원합니다. 노트북을 만들 때 언어를 선택할 수 있지만 언제든지 변경할 수 있습니다.
# MAGIC
# MAGIC 기본 언어는 페이지 상단의 노트북 제목 바로 오른쪽에 나타납니다. 이 과정에서는 SQL과 Python 노트북을 함께 사용합니다.
# MAGIC
# MAGIC 이 노트북의 기본 언어를 SQL로 변경하겠습니다.
# MAGIC
# MAGIC 단계:
# MAGIC * 화면 상단 노트북 제목 옆 **Python** 클릭
# MAGIC * 팝업되는 UI의 드롭다운 목록에서 **SQL** 선택
# MAGIC
# MAGIC **참고**: 이 셀 바로 앞의 셀에 <strong><code>&#37;python</code></strong>이라는 새 줄이 표시되어야 합니다. 잠시 후에 이에 대해 논의하겠습니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Multi Language Support with Magic Command!  
# MAGIC 매직컴맨드 **%** 를 사용해서 하나의 노트북에서 다양한 언어를 사용해서 코딩할 수 있습니다. 
# MAGIC
# MAGIC * **&percnt;python** 
# MAGIC * **&percnt;scala** 
# MAGIC * **&percnt;sql** 
# MAGIC * **&percnt;r** 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ### SQL 셀 생성하고 실행하기 
# MAGIC
# MAGIC * 이 셀을 선택 하고 키보드의 **b** 버튼을 눌러 아래에 새 셀을 만듭니다.
# MAGIC * 다음 코드를 아래 셀에 복사한 후 셀을 실행합니다.
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **NOTE**: GUI 옵션 및 키보드 단축키를 포함하여 셀을 추가, 이동 및 삭제하는 다양한 방법이 있습니다. 자세한 사항은 <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">문서</a> 를 참고하세요

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "I'm running SQL!"

# COMMAND ----------

# DBTITLE 1,Python 코드 실행하기
print("Hello Python!")

# COMMAND ----------

# DBTITLE 1,Scala 코드 실행하기
# MAGIC %scala 
# MAGIC println("Hello Scala")

# COMMAND ----------

# DBTITLE 1,R 코드 실행하기
# MAGIC %r
# MAGIC print("Hello R!", quote=FALSE)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Markdown
# MAGIC
# MAGIC 마법 명령 **%md**를 사용하면 셀에서 Markdown을 렌더링할 수 있습니다.
# MAGIC * 편집을 시작하려면 이 셀을 두 번 클릭합니다.
# MAGIC * 그런 다음 **`Esc`** 를 눌러 편집을 중지합니다.
# MAGIC
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC
# MAGIC 이것은 기본 텍스트 입니다.
# MAGIC
# MAGIC 이것은 **굵은** 단어가 포함된 텍스트입니다.
# MAGIC
# MAGIC *기울임꼴* 단어가 포함된 텍스트입니다.
# MAGIC
# MAGIC 이것은 순차 리스트 입니다
# MAGIC 1. once
# MAGIC 1. two
# MAGIC 1. three
# MAGIC
# MAGIC 이것은 리스트 입니다. 
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC 이미지:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC 테이블 포맷:
# MAGIC
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.다양한 Utility 지원 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### %sh
# MAGIC * 매직커맨드 **%sh**을 사용하여 Spark 드라이버 노드에서 sh 커맨드를 수행합니다.

# COMMAND ----------

# MAGIC %sh 
# MAGIC
# MAGIC ps | grep 'java'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ### %run
# MAGIC * 매직커맨드 **%run**을 사용하여 노트북에서 다른 노트북을 실행할 수 있습니다.
# MAGIC * 실행할 노트북은 상대 경로로 지정됩니다.
# MAGIC * 참조된 노트북은 현재 노트북의 일부인 것처럼 실행되므로 호출 노트북에서 temporary views 및 기타 로컬 변수 선언을 사용할 수 있습니다.

# COMMAND ----------

# DBTITLE 1,%run 다른 노트북을 수행합니다
# MAGIC %run "../Includes/Setup"

# COMMAND ----------

# DBTITLE 1,Setup 노트북에서 정의한 변수값을 불러올 수 있습니다
print(databricks_user)

# COMMAND ----------

# MAGIC  
# MAGIC %md 
# MAGIC ### Widget 을 이용한 동적인 변수 활용
# MAGIC
# MAGIC Databrick utilites (e.g. `dbutils`) 은 notebook에서 유용하게 사용할 수 있는 다양한 기능을 제공합니다: 
# MAGIC https://docs.databricks.com/dev-tools/databricks-utils.html
# MAGIC
# MAGIC 그중 하나인 **"Widgets"** 기능은 노트북에서 동적인 변수처리를 손쉽게 할 수 있도록 도와줍니다:https://docs.databricks.com/notebooks/widgets.ht

# COMMAND ----------

#아래 주석을 제거하고 실행하면 현재 노트북에 생성된 위젯을 삭제합니다.
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("dropdown_widget", "1", [str(x) for x in range(1, 4)])

# COMMAND ----------

# DBTITLE 1,상단에 생성된 dropdown_widget을 다른 값으로 선택 후 수행
print("dropdown_widget 의 현재값은 :", dbutils.widgets.get("dropdown_widget"))

# COMMAND ----------

dbutils.widgets.text("text_widget","Hello World!")

# COMMAND ----------

# DBTITLE 1,상단에 생성된 text_widget 을 값을 변경 후 수행
print(dbutils.widgets.get("text_widget"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Databricks Utilities
# MAGIC
# MAGIC Databricks 노트북은 환경 구성 및 상호 작용을 위한 다양한 유틸리티 명령을 제공합니다. <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils 문서</a>
# MAGIC
# MAGIC 이 과정에서 종종 **`dbutils.fs.ls()`** 를 사용하여 Python 셀에서 파일 디렉토리를 나열할 것입니다.

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/iot-stream/data-device")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Magic Command: &percnt;fs
# MAGIC **&percnt;fs** 매직커맨드는 `dbutils.fs` 에 대한 wrapper 로  ,`dbutils.fs.ls("/databricks-datasets")` 와 `%fs ls /databricks-datasets`는 똑같이 동작합니다.  
# MAGIC 이 노트북에서는 설치시에 기본으로 제공되는 /databricks-datasets 상의 데이터를 사용해 보도록 하겠습니다. 

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/iot-stream/data-device

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ### display()
# MAGIC
# MAGIC 셀에서 SQL 쿼리를 실행할 때 결과는 항상 렌더링된 표 형식으로 표시됩니다.
# MAGIC
# MAGIC Python 셀에서 반환한 테이블 형식 데이터가 있는 경우 **`display`** 를 호출하여 동일한 유형의 미리보기를 얻을 수 있습니다.
# MAGIC
# MAGIC 여기서 파일 시스템의 이전 list 명령을 **`display`** 로 래핑합니다.

# COMMAND ----------

path = "/databricks-datasets/iot-stream/data-device"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. 데이터 가지고 놀기

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pyspark 을 사용한 데이터 탐색

# COMMAND ----------

# DBTITLE 1,pyspark dataframe으로 csv파일 읽기
datapath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamondsDF = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .load(datapath)

# COMMAND ----------

# DBTITLE 1,CSV 파일 내용 확인
# MAGIC %fs
# MAGIC head /databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv

# COMMAND ----------

display(diamondsDF)

# COMMAND ----------

from pyspark.sql.functions import avg
# groupBy 를 사용해서 color별 평균 가격 확인
display(diamondsDF.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

# COMMAND ----------

# DBTITLE 1,현재 사용 중인 데이터베이스 이름 확인
# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL 을 사용한 데이터 탐색

# COMMAND ----------

# DBTITLE 1,SQL 을 사용한 diamonds csv 데이터셋 탐색
# MAGIC %sql 
# MAGIC
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC -- table로 생성한 내용은 왼쪽 Data 메뉴에서 확인이 가능합니다. 
# MAGIC CREATE TABLE diamonds
# MAGIC USING csv
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true");
# MAGIC
# MAGIC
# MAGIC SELECT * FROM diamonds;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. 다양한 협업 기능  
# MAGIC   
# MAGIC   
# MAGIC     
# MAGIC 데이터브릭스 노트북은 데이터에 대한 인사이트를 찾아가는 과정을 기업내 다양한 데이터팀이 함께 협업을 통해서 가속화할 수 있도록 다양한 협업 기능과 동시에 Enterprise급 권한 관리 기능을 제공합니다. 
# MAGIC - Comments
# MAGIC - Revision history
# MAGIC - Coediting like Google Docs
# MAGIC - Fine Grain Access Controls for Workspace, Notebook, Clusters, Jobs, and Tables

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC ## 6. Downloading Notebooks
# MAGIC
# MAGIC 개별 노트북 또는 노트북 컬렉션을 다운로드하기 위한 다양한 옵션이 있습니다.
# MAGIC
# MAGIC 여기에서 이 노트북과 이 과정의 모든 노트북 모음을 다운로드하는 프로세스를 진행합니다.
# MAGIC
# MAGIC ### 단일 노트북 다운로드
# MAGIC
# MAGIC 단계:
# MAGIC * 노트북 상단의 클러스터 선택 오른쪽에 있는 **File** 옵션을 클릭합니다.
# MAGIC * 표시되는 메뉴에서 **Export** 위로 마우스를 가져간 다음 **Source file**을 선택합니다.
# MAGIC
# MAGIC 노트북이 개인 노트북으로 다운로드됩니다. 현재 노트북 이름으로 이름이 지정되고 기본 언어에 대한 파일 확장자를 갖습니다. 파일 편집기로 이 노트북을 열고 Databricks 노트북의 원시 콘텐츠를 볼 수 있습니다.
# MAGIC
# MAGIC 이러한 소스 파일은 모든 Databricks 작업 영역에 업로드할 수 있습니다.
# MAGIC
# MAGIC ### 노트북 컬렉션 다운로드 
# MAGIC
# MAGIC **NOTE**: 다음 지침에서는 **Workspace**를 사용하여 이러한 자료를 가져온 것으로 가정합니다.
# MAGIC
# MAGIC 단계:
# MAGIC * 왼쪽 사이드바에서 ![](https://files.training.databricks.com/images/workspace-icon.png) **Workspace**를 클릭합니다.
# MAGIC    * 이것은 이 노트북에 대한 상위 디렉토리의 미리보기를 제공합니다.
# MAGIC * 화면 중앙 부근의 디렉토리 미리보기 왼쪽에 왼쪽 화살표(<)가 있어야 합니다. 파일 계층 구조에서 위로 이동하려면 이것을 클릭하십시오.
# MAGIC * **Data Engineering with Databricks**라는 디렉터리가 표시되어야 합니다. 아래쪽 화살표를 클릭하여 메뉴를 불러옵니다.
# MAGIC * 메뉴에서 **Export** 위로 마우스를 가져간 다음 **DBC Archive**를 선택합니다.
# MAGIC
# MAGIC 다운로드되는 DBC(Databricks Cloud) 파일에는 이 과정의 디렉터리 및 노트북 모음이 압축되어 포함되어 있습니다. 사용자는 이러한 DBC 파일을 로컬에서 편집하려고 시도해서는 안 되지만 Databricks 작업 영역에 안전하게 업로드하여 노트북 콘텐츠를 이동하거나 공유할 수 있습니다.
# MAGIC
# MAGIC **NOTE**: DBC 컬렉션을 다운로드할 때 결과 미리 보기 및 플롯도 내보냅니다. 반면 Source file 형태로 노트북 다운로드 시 코드만 저장됩니다.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
