// Databricks notebook source
// MAGIC
// MAGIC %md-sandbox
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Create Service Principal
// MAGIC    * In Azure Active Directory, go to Properties. Make note of the **Directory ID**.
// MAGIC    * Go to App Registrations and create a New application registration
// MAGIC       * example: airlift-app-registration, Web app/API, https://can-be-literally-anything.com
// MAGIC    * Make note of the **Application ID**.
// MAGIC    * Under Settings > Keys, create and copy a new key. Make note of the **Key Value**.
// MAGIC 1. Create Storage Account
// MAGIC    * On the Advanced Tab (1), make sure to enable Hierarchal NameSpace (2).
// MAGIC       <img src="https://www.evernote.com/l/AAFW89nF7OtKb4j798yshtao-a4SVE2vUk4B/image.png" width=300px>
// MAGIC    * Make note of the **Storage Account Name**.
// MAGIC    * Create a Data Lake Gen2 file system on the storage account and make note of the **File System Name**.
// MAGIC    * Under Access control (IAM) add a *Role assignment*, where the role is *Storage Blob Data Contributor (Preview)* assigned to the App Registration previously created.

// COMMAND ----------

// MAGIC %fs ls /mnt/adlsfs

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
