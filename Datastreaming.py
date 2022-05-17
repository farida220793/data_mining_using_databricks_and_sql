# Databricks notebook source
# MAGIC %fs 
# MAGIC ls databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/")

# COMMAND ----------

files

# COMMAND ----------

sizeList = [x.size for x in files]


# COMMAND ----------

sizeList

# COMMAND ----------

pathList = [x.path for x in files]
pathList

# COMMAND ----------

filesRDD = sc.parallelize(pathList)
filesRDD.count()

# COMMAND ----------

filesRDD.collect()

# COMMAND ----------

csvFilesRDD =filesRDD.filter(lambda x: x.endswith('.csv'))

# COMMAND ----------

csvFilesRDD.count()

# COMMAND ----------

import re
# Assumes input MM -DD - YYYY .csv
def convert_name(filename):
    m = re.match("^.*([0 -9]{2})-([0 -9]{2})-([0 -9]{4}).csv$", filename)
    return int(m.group(3) + m.group(1) + m.group(2))

# COMMAND ----------

csvPairRDD = csvFilesRDD.map(lambda x: (convert_name(x), x))

# COMMAND ----------

csvPairRDD.take(1)

# COMMAND ----------

sosrtcv = csvPairRDD.sortByKey(False)

# COMMAND ----------

sosrtcv.collect()

# COMMAND ----------

chosenfile = ('dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports_us/03-11-2021.csv')

# COMMAND ----------

df = spark.read.options(header='True').csv(chosenfile)

# COMMAND ----------

df.display()

# COMMAND ----------


