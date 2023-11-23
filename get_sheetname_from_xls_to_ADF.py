# Databricks notebook source
#pip install xlrd==1.2.0
#pip install openpyxl
!pip install openpyxl
import openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
ref=pd.ExcelFile('https://databrickssessionstorage.blob.core.windows.net/databricksparkassignment/Input/Customer_Sheets.xlsx')
y=ref.sheet_names
dbutils.notebook.exit(y)

# COMMAND ----------

import pandas as pd
import openpyxl
# getting file input from adf pipeline
dbutils.widgets.text("input",",")
y=dbutils.widgets.get("input")
print(y)
ref=pd.ExcelFile(y)
sheet_names=ref.sheetnames
dbutils.notebook.exit(sheet_names)

# COMMAND ----------


