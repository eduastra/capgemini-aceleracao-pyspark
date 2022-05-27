from ast import alias
from decimal import Rounded
from math import ceil
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import regexp_replace

sc = SparkContext()
spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

sch = StructType([
    StructField("InvoiceNo",  IntegerType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description",  StringType(), True),
    StructField("quantity",  IntegerType(), True),
    StructField("InvoiceDate",  StringType(), True),
    StructField("UnitPrice",   StringType(), True),
    StructField("CustomerID",  StringType(), True),
	StructField("Country",  StringType(), True)
])

df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
				  .schema(sch)
		          .load("/home/spark/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

def typeChange(df, col, type):
	df_new = df.withColumn(
		col, 
			F.col(col)
			.cast(type)
		)
	return df_new	

def isNA(df, col):
	df_new = df.withColumn(col, 
		F.when(
				(
					(F.col(col).isNull()) |
					(F.col(col) == "NA")
				), 0
			).otherwise(F.col(col))
		)
	return 			  

df = df.withColumn('UnitPrice', regexp_replace('UnitPrice', ',', '.'))
df = typeChange(df, "UnitPrice", "float")
df = df.withColumn('Value', (F.col('Quantity') * F.col('UnitPrice')))
df = typeChange(df, 'Value', "float")
df = typeChange(df, "Quantity", "int")
#df.printSchema()

def quest1(df):
	df_filtered = df.select("StockCode", "Quantity", "UnitPrice").where(
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0))
	df_filtered.groupBy().sum('Quantity')

def quest2(df):
	df_final = (df.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
       .withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm'))
      )
	df_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
		).where(
			(F.col('StockCode').rlike("^gift_0001")) & 
			(F.col("UnitPrice") > 0)
		)
	df_filtered.groupBy(F.month(F.col("InvoiceDate")).alias("mês")).count().orderBy('mês').show()

def quest3(df):
	df_final = df.select("*").where(F.col("StockCode") == "S")
	df_final.groupBy("StockCOde").count().show()

def quest4(df):
	#df_final = df.select("*").where(~F.col('InvoiceNo').rlike("C"))
	(df.groupBy("StockCode")
	.sum('Quantity')
	.alias("Total Quantity")
	.orderBy('Total Quantity.sum(Quantity)', ascending=False)
	.show(1))

def quest5(df):
	df_final = (df.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
		.withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm'))
		)
	df_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(F.col("UnitPrice") > 0)
			)
	(df_filtered
	.groupBy(F.month(F.col("InvoiceDate")).alias("mês")
		).sum('Quantity')
	.alias("Total Quantity")
	.orderBy("Total Quantity.sum(Quantity)")
	.show())

def quest6(df):
	df_final = (df.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
		.withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm'))
		)
	df_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(F.col("UnitPrice") > 0)
			)
	(df_filtered
	.groupBy(
		F.hour(F.col("InvoiceDate")).alias("hora")
		).sum('UnitPrice')
	.alias("Total UnitPrice")
	.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
	.show(1))

def quest7(df):
	df_final = (df.withColumn("InvoiceDate", F.lpad(F.col('InvoiceDate'), 16, '0'))
		.withColumn("InvoiceDate",F.to_timestamp(F.col("InvoiceDate"), 'd/M/yyyy HH:mm'))
		)
	df_filtered = df_final.select(
		"StockCode", "Quantity", "UnitPrice", "InvoiceDate"
			).where(
				(F.col("UnitPrice") > 0)
			)
	(df_filtered
	.groupBy(
		F.month(F.col("InvoiceDate")).alias("mês")
		).sum('UnitPrice')
	.alias("Total UnitPrice")
	.orderBy("Total UnitPrice.sum(UnitPrice)", ascending=False)
	.show(1))

def quest8(df): #rever
	df =df.groupBy('Description', F.year('InvoiceDate').alias('year'), F.month('InvoiceDate').alias('month')
	).agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor')
	).orderBy(F.col('valor').desc()).dropDuplicates(['month']).show()

def quest9(df):
	df = df.groupBy('Country'
	).agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor')
	).orderBy(F.col('valor').desc()
	).limit(1).show()

def quest10(df):
	df = df.where((F.col('StockCode')== 'M') & (~F.col('InvoiceNo').rlike('C'))
	).groupBy('Country'
	).agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor')
	).orderBy(F.col('valor').desc()
	).limit(1).show()

def quest11(df):
	df = df.groupBy('InvoiceNo'
	).agg(F.round(F.sum(F.col('UnitPrice') * F.col('Quantity')), 2).alias('valor')
	).orderBy(F.col('valor').desc()
	).limit(1).show()

def quest12(df):
	df = df.groupBy('InvoiceNo'
	).agg(F.max("Quantity").alias('quantidade')
	).orderBy(F.col('quantidade').desc()
	).limit(1).show()

def quest13(df):
	(df.where(F.col("CustomerID").isNotNull())
	.groupBy(F.col('CustomerID').alias('customer'))
	.count()
	.orderBy(F.col('count').desc())
	.limit(1)
	.show())

quest10(df)
#Para determinar valor da venda considere: Quantity * UnitPrice
#Desconsidere StockCode = P ADS
#Quantity negativa representa valor que saiu do caixa e não podem ser considerados como venda