# Databricks notebook source
# MAGIC %md # Exercise 5 - Task 2: Creating dashboards in Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 데이터 브릭 대시 보드를 사용하면 그래프와 시각화를 게시하고 조직과 프레젠테이션 형식으로 공유 할 수 있습니다. 이 노트북은 이러한 대시 보드를 만드는 방법을 보여줍니다.
# MAGIC
# MAGIC 1. 이 메뉴에서 각 셀을 실행하려면 위 메뉴에서 ** Run All ** 을 선택하십시오. 셀은 각 쿼리에 대한 시각화를 렌더링하도록 구성되었습니다. 각 쿼리의 출력 아래에서 ** Plot Options ** 를 선택하여 이들 각각을 검사 할 수 있습니다. 또한 데이터의 다른 시각화를 자유롭게 사용하고 사기 거래에 흥미로운 시각적 요소를 제공 할 수있는 추가 쿼리를 작성할 수 있습니다.
# MAGIC
# MAGIC 2. 완료되면 위 메뉴에서 **보기** 를 선택한 다음 ** + 새 대시 보드 ** 를 선택하십시오.
# MAGIC
# MAGIC 3. 새 대시 보드보기에서 오른쪽에 ** 의심스러운 거래 ** 와 같은 대시 보드 제목을 입력하십시오.
# MAGIC
# MAGIC   ![The dashboard details pane is displayed, with Suspicious Transactions entered in the title field.](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-dashboards-suspicious-transactions-create.png "Create Databricks Dashboard")
# MAGIC
# MAGIC 4. 대시 보드보기에서이 단계가 포함 된 셀을 제거한 다음 Exercise 5 : 데이터 브릭 대시 보드 제목을 끌어서 대시 보드의 전체 너비로 확장하십시오. 최종 대시 보드는 다음과 유사합니다.
# MAGIC
# MAGIC   ![The layout for the Suspicious Transactions dashboard is displayed.](https://github.com/Microsoft/MCW-Cosmos-DB-Real-Time-Advanced-Analytics/raw/master/Hands-on%20lab/media/databricks-dashboards-suspicious-transactions.png "Suspicious Transactions Dashboard")
# MAGIC   
# MAGIC 5. 오른쪽에서 ** 현재 대시 보드 ** 를 선택하면 대시 보드의 전체 화면보기가 표시됩니다.
# MAGIC
# MAGIC 6. 완료되면 대시 보드 왼쪽 상단에서 ** 종료 **를 선택하십시오.
# MAGIC
# MAGIC 7. 이것으로 Databricks 대시 보드 작업이 완료되었습니다. Cosmos DB real-time advanced analytics hands-on lab으로 돌아가 다음 작업을 계속하십시오.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cvvVerifyResult, isSuspicious FROM scored_transactions WHERE isSuspicious = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT localHour, isSuspicious FROM scored_transactions WHERE isSuspicious = 1 ORDER BY localHour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT accountState, isSuspicious FROM scored_transactions WHERE paymentBillingCountryCode = 'US'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ipCountryCode, transactionAmountUSD FROM scored_transactions
