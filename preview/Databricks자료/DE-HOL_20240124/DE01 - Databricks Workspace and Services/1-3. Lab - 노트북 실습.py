# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="5f2cfc0b-1998-4182-966d-8efed6020eb2"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Databricks 플랫폼 시작하기
# MAGIC
# MAGIC 이 노트북은 Databricks 데이터 과학 및 엔지니어링 작업 영역의 기본 기능 중 일부에 대한 실습 검토를 제공합니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 실습을 마치면 다음을 수행할 수 있습니다.
# MAGIC - 노트북 이름 바꾸기 및 기본 언어 변경
# MAGIC - 클러스터 연결
# MAGIC - **`%run`** 마법 명령 사용
# MAGIC - Python 및 SQL 셀 실행
# MAGIC - Markdown 셀 만들기

# COMMAND ----------

# MAGIC %md <i18n value="05dca5e4-6c50-4b39-a497-a35cd6d99434"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # 노트북 이름 바꾸기
# MAGIC
# MAGIC 노트북의 이름을 변경하는 것은 쉽습니다. 이 페이지 상단 왼쪽의 노트북 
# MAGIC 이름을 클릭한 다음 이름을 변경합니다. 필요한 경우 나중에 이 노트북으로 다시 쉽게 이동할 수 있도록 기존 이름 끝에 짧은 테스트 문자열을 추가합니다.

# COMMAND ----------

# MAGIC %md <i18n value="f07b8dd7-436d-4719-9c17-18cd47f493fe"/>
# MAGIC
# MAGIC
# MAGIC # 클러스터 연결
# MAGIC
# MAGIC 노트북의 셀을 실행하려면 클러스터에서 제공하는 컴퓨팅 리소스가 필요합니다. 노트북에서 셀을 처음 실행하면 클러스터가 아직 연결되지 않은 경우 클러스터에 연결하라는 메시지가 표시됩니다.
# MAGIC
# MAGIC 이 페이지의 오른쪽 상단 모서리 근처에 있는 드롭다운을 클릭하여 지금 이 노트북에 클러스터를 연결합니다. 이전 실습에서 만든 클러스터를 선택합니다. 이렇게 하면 노트북의 실행 상태가 지워지고 노트북이 선택한 클러스터에 연결됩니다.
# MAGIC
# MAGIC 드롭다운 메뉴는 필요에 따라 클러스터를 시작하거나 다시 시작할 수 있는 옵션을 제공합니다. 한 번의 이동으로 클러스터를 분리하고 다시 연결할 수도 있습니다. 필요할 때 실행 상태를 지우는 데 유용합니다.

# COMMAND ----------

# MAGIC %md <i18n value="68805a5e-3b2c-4f79-819f-273d4ca95137"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # %run 사용하기
# MAGIC
# MAGIC 모든 유형의 복잡한 프로젝트는 이를 더 간단하고 재사용 가능한 구성 요소로 분해하는 기능을 통해 이점을 얻을 수 있습니다.
# MAGIC
# MAGIC Databricks 노트북의 맥락에서 이 기능은 **`%run`** 마법 명령을 통해 제공됩니다.
# MAGIC
# MAGIC 이러한 방식으로 사용하면 변수, 함수 및 코드 블록이 현재 프로그래밍 컨텍스트의 일부가 됩니다.
# MAGIC
# MAGIC 다음 예재를 고려하십시오.
# MAGIC
# MAGIC **`Notebook_A`** 에는 네 가지 명령이 있습니다.
# MAGIC   1. **`name = "John"`**
# MAGIC   2. **`print(f"Hello {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Welcome back {full_name}`**
# MAGIC
# MAGIC **`Notebook_B`** 에는 하나의 명령만 있습니다.
# MAGIC    1. **`full_name = f"{name} Doe"`**
# MAGIC
# MAGIC **`Notebook_B`** 를 실행하면 변수 **`name`** 이 **`Notebook_B`** 에 정의되어 있지 않기 때문에 실행에 실패합니다.
# MAGIC
# MAGIC 마찬가지로 **`Notebook_A`** 가 **`Notebook_A`** 에 정의되지 않은 **`full_name`** 변수를 사용하기 때문에 **`Notebook_A`** 가 실패할 것이라고 생각할 수 있지만 그렇지 않습니다!
# MAGIC
# MAGIC 실제로 일어나는 일은 아래에서 볼 수 있듯이 두 개의 노트북이 함께 병합되고 **그런 다음** 실행됩니다.
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC
# MAGIC 따라서 노트북은 일반적으로 예상되는 동작을 제공합니다.
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**

# COMMAND ----------

# MAGIC %md <i18n value="260e99b3-4126-41b7-8210-b6ff01b98790"/>
# MAGIC
# MAGIC
# MAGIC 이 노트북이 포함된 폴더에는 **`ExampleSetupFolder`** 라는 하위 폴더가 있으며, 여기에는 **`example-setup`** 이라는 노트북이 들어 있습니다.
# MAGIC
# MAGIC 이 간단한 노트북은 **`my_name`** 변수를 선언하고 **`None`** 으로 설정한 다음 **`example_df`** 라는 DataFrame을 생성합니다.
# MAGIC
# MAGIC 예제 아래 셀에 있는 `example-setup` 설정 노트북을 열고 이름이 **`None`** 이 아니라 따옴표로 묶인 실습자의 이름(또는 다른 사람의 이름)이 되도록 수정하고 다음 두 셀이 **`AssertionError`** 를 발생시키지 않고 실행되도록 합니다.

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

# DBTITLE 1,%run 실습
assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# MAGIC %md <i18n value="ece094f7-d013-4b24-aa54-e934f4ab7dbd"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Python 셀 실행하기
# MAGIC
# MAGIC 다음 셀을 실행하여 **`example_df`** Dataframe을 표시하여 **`example-setup`** 노트북이 실행되었는지 확인합니다. 이 테이블은 값이 증가하는 16개 행으로 구성됩니다.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md <i18n value="ce392afd-2e73-4a51-adc4-7d654dad6215"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # 프로그래밍 언어 변경
# MAGIC
# MAGIC 이 노트북의 기본 언어는 Python으로 설정되어 있습니다. 노트북 이름 오른쪽에 있는 **Python** 버튼을 클릭하여 이를 변경합니다. 기본 언어를 SQL로 변경합니다.
# MAGIC
# MAGIC Python 셀 앞에 <strong><code>&#37;python</code></strong> 매직 명령이 자동으로 추가되어 해당 셀의 유효성을 유지합니다. 이 작업은 실행 상태도 지웁니다.

# COMMAND ----------

# MAGIC %md <i18n value="dfce7fd1-08e8-4cc3-92ac-a2eb74f804ef"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Markdown 셀 생성
# MAGIC
# MAGIC 이 셀 아래에 새로운 셀을 추가합니다. 적어도 다음 요소를 포함하는 일부 Markdown으로 채웁니다.
# MAGIC * A header
# MAGIC * Bullet points
# MAGIC * A link (using your choice of HTML or Markdown conventions)

# COMMAND ----------

# DBTITLE 1,Markdown 실습
# MAGIC %md
# MAGIC #TODO 
# MAGIC
# MAGIC * first point
# MAGIC * second point
# MAGIC * third point
# MAGIC
# MAGIC An [example link](http://www.databricks.com/kr/ "데이터브릭스 한국어 사이트") in a sentence.
# MAGIC

# COMMAND ----------

# MAGIC %md <i18n value="a54470bc-2a69-4a34-acbb-fe28c4dee284"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## SQL 셀 수행
# MAGIC
# MAGIC SQL을 사용하여 델타 테이블을 쿼리하려면 다음 셀을 실행하십시오. 이것은 모든 DBFS 설치에 포함된 Databricks 제공 예제 데이터 세트가 지원하는 테이블에 대해 간단한 쿼리를 실행합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled/`

# COMMAND ----------

# MAGIC %md <i18n value="7499c6b6-b3f3-4641-88d9-5a260d3c11f8"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC 이 테이블을 구성하는 파일을 보려면 다음 셀을 실행하십시오.

# COMMAND ----------

files = dbutils.fs.ls(f"/databricks-datasets/nyctaxi-with-zipcodes/subsampled/")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="a17b5667-53bc-4f8a-8601-5599f4ebb819"/>
# MAGIC
# MAGIC # 노트북 상태 지우기
# MAGIC
# MAGIC 때로는 노트북에 정의된 모든 변수를 지우고 처음부터 시작하는 것이 유용합니다. 이는 셀을 격리하여 테스트하거나 단순히 실행 상태를 재설정하려는 경우에 유용할 수 있습니다.
# MAGIC
# MAGIC 상단의 **Run** 메뉴를 방문하여 **Clear State & outputs** 를 선택합니다.
# MAGIC
# MAGIC 이제 아래 셀을 실행해 보고 위의 이전 셀을 다시 실행할 때까지 이전에 정의된 변수가 더 이상 정의되지 않음을 확인하십시오.

# COMMAND ----------

# DBTITLE 1,노트북 상태 지우기 실습
print(my_name)

# COMMAND ----------

# MAGIC %md <i18n value="8bff18c2-3ecf-484a-9a8c-dadab7eaf0a1"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC # 변경 사항 리뷰
# MAGIC
# MAGIC 이 페이지의 오른쪽 사이드바에 있는 되돌리기(revision history)<img src="https://docs.databricks.com/_images/revision-history.png"> 버튼을 클릭하여 대화 상자를 엽니다. 세 가지 변경 사항이 표시됩니다.
# MAGIC 1. 이전 노트북 이름으로 **Removed**
# MAGIC 1. 새로운 노트북 이름으로 **Added**
# MAGIC 1. 위 마크다운 셀 생성을 위해 **Modified**
# MAGIC
# MAGIC 대화 상자를 사용하여 변경 사항을 되돌리고 이 노트북을 원래 상태로 복원합니다.

# COMMAND ----------

# MAGIC %md <i18n value="cb3c335a-dd4c-4620-9f10-6946250f2e02"/>
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Wrapping Up
# MAGIC
# MAGIC 이 실습을 완료하면 이제 노트북 조작, 새로운 셀 생성, 노트북 내에서 노트북 실행에 익숙함을 느낄 수 있습니다.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
