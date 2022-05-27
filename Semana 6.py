from email.header import Header
from unittest import result
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
schema_communities_crime = StructType([
		StructField("state", 		 FloatType(), True),
		StructField("county", 		 IntegerType(), True),
		StructField("community", 	 IntegerType(), True),
		StructField("communityname", StringType(), True),
		StructField("fold", 		 StringType(), True),
		StructField("population", 	 FloatType(), True),
		StructField("householdsize", FloatType(), True),
		StructField("racepctblack",  FloatType(), True),
		StructField("racePctWhite",  FloatType(), True),
		StructField("racePctAsian",  FloatType(), True),
		StructField("racePctHisp",   FloatType(), True),
		StructField("agePct12t21",   FloatType(), True),
		StructField("agePct12t29",   FloatType(), True),
		StructField("agePct16t24",   FloatType(), True),
		StructField("agePct65up", 	 FloatType(), True),
		StructField("numbUrban", 	 FloatType(), True),
		StructField("pctUrban", 	 FloatType(), True),
		StructField("medIncome", 	 FloatType(), True),
		StructField("pctWWage",      FloatType(), True),
		StructField("pctWFarmSelf",  FloatType(), True),
		StructField("pctWInvInc",    FloatType(), True),
		StructField("pctWSocSec", 	 FloatType(), True),
		StructField("pctWPubAsst",   FloatType(), True),
		StructField("pctWRetire", 	 FloatType(), True),
		StructField("medFamInc", 	 FloatType(), True),
		StructField("perCapInc", 	 FloatType(), True),
		StructField("whitePerCap",   FloatType(), True),
		StructField("blackPerCap",   FloatType(), True),
		StructField("indianPerCap",  FloatType(), True),
		StructField("AsianPerCap",   FloatType(), True),
		StructField("OtherPerCap",   FloatType(), True),
		StructField("HispPerCap",    FloatType(), True),
		StructField("NumUnderPov",   FloatType(), True),
		StructField("PctPopUnderPov",   FloatType(), True),
		StructField("PctLess9thGrade",  FloatType(), True),
		StructField("PctNotHSGrad",     FloatType(), True),
		StructField("PctBSorMore", 	    FloatType(), True),
		StructField("PctUnemployed",    FloatType(), True),
		StructField("PctEmploy", 	    FloatType(), True),
		StructField("PctEmplManu", 	    FloatType(), True),
		StructField("PctEmplProfServ",  FloatType(), True),
		StructField("PctOccupManu",     FloatType(), True),
		StructField("PctOccupMgmtProf", FloatType(), True),
		StructField("MalePctDivorce",   FloatType(), True),
		StructField("MalePctNevMarr",   FloatType(), True),
		StructField("FemalePctDiv",     FloatType(), True),
		StructField("TotalPctDiv",   	FloatType(), True),
		StructField("PersPerFam", 	 	FloatType(), True),
		StructField("PctFam2Par", 	 	FloatType(), True),
		StructField("PctKids2Par", 	 	FloatType(), True),
		StructField("PctYoungKids2Par", FloatType(), True),
		StructField("PctTeen2Par", 	 	FloatType(), True),
		StructField("PctWorkMomYoungKids", FloatType(), True),
		StructField("PctWorkMom", 		   FloatType(), True),
		StructField("NumIlleg",			   FloatType(), True),
		StructField("PctIlleg", 		   FloatType(), True),
		StructField("NumImmig", 		   FloatType(), True),
		StructField("PctImmigRecent", 	   FloatType(), True),
		StructField("PctImmigRec5", 	   FloatType(), True),
		StructField("PctImmigRec8", 	   FloatType(), True),
		StructField("PctImmigRec10", 	   FloatType(), True),
		StructField("PctRecentImmig", 	   FloatType(), True),
		StructField("PctRecImmig5",        FloatType(), True),
		StructField("PctRecImmig8",        FloatType(), True),
		StructField("PctRecImmig10",       FloatType(), True),
		StructField("PctSpeakEnglOnly",    FloatType(), True),
		StructField("PctNotSpeakEnglWell", FloatType(), True),
		StructField("PctLargHouseFam",     FloatType(), True),
		StructField("PctLargHouseOccup",   FloatType(), True),
		StructField("PersPerOccupHous",    FloatType(), True),
		StructField("PersPerOwnOccHous",   FloatType(), True),
		StructField("PersPerRentOccHous",  FloatType(), True),
		StructField("PctPersOwnOccup",     FloatType(), True),
		StructField("PctPersDenseHous",    FloatType(), True),
		StructField("PctHousLess3BR", 	   FloatType(), True),
		StructField("MedNumBR",         FloatType(), True),
		StructField("HousVacant",       FloatType(), True),
		StructField("PctHousOccup",     FloatType(), True),
		StructField("PctHousOwnOcc",    FloatType(), True),
		StructField("PctVacantBoarded", FloatType(), True),
		StructField("PctVacMore6Mos",   FloatType(), True),
		StructField("MedYrHousBuilt",   FloatType(), True),
		StructField("PctHousNoPhone",   FloatType(), True),
		StructField("PctWOFullPlumb",   FloatType(), True),
		StructField("OwnOccLowQuart",   FloatType(), True),
		StructField("OwnOccMedVal", 	FloatType(), True),
		StructField("OwnOccHiQuart", 	FloatType(), True),
		StructField("RentLowQ", 		FloatType(), True),
		StructField("RentMedian", 		FloatType(), True),
		StructField("RentHighQ", 		FloatType(), True),
		StructField("MedRent", 			FloatType(), True),
		StructField("MedRentPctHousInc",     FloatType(), True),
		StructField("MedOwnCostPctInc",      FloatType(), True),
		StructField("MedOwnCostPctIncNoMtg", FloatType(), True),
		StructField("NumInShelters", 		 FloatType(), True),
		StructField("NumStreet", 			 FloatType(), True),
		StructField("PctForeignBorn", 		 FloatType(), True),
		StructField("PctBornSameState", 	 FloatType(), True),
		StructField("PctSameHouse85", 		 FloatType(), True),
		StructField("PctSameCity85", 		 FloatType(), True),
		StructField("PctSameState85",		 FloatType(), True),
		StructField("LemasSwornFT", 		 FloatType(), True),
		StructField("LemasSwFTPerPop", 		 FloatType(), True),
		StructField("LemasSwFTFieldOps", 	 FloatType(), True),
		StructField("LemasSwFTFieldPerPop", FloatType(), True),
		StructField("LemasTotalReq", 		FloatType(), True),
		StructField("LemasTotReqPerPop", 	FloatType(), True),
		StructField("PolicReqPerOffic",		FloatType(), True),
		StructField("PolicPerPop", 			FloatType(), True),
		StructField("RacialMatchCommPol",   FloatType(), True),
		StructField("PctPolicWhite", 		FloatType(), True),
		StructField("PctPolicBlack", 		FloatType(), True),
		StructField("PctPolicHisp", 		FloatType(), True),
		StructField("PctPolicAsian", 		FloatType(), True),
		StructField("PctPolicMinor", 		FloatType(), True),
		StructField("OfficAssgnDrugUnits",  FloatType(), True),
		StructField("NumKindsDrugsSeiz", 	FloatType(), True),
		StructField("PolicAveOTWorked",		FloatType(), True),
		StructField("LandArea", 			FloatType(), True),
		StructField("PopDens", 				FloatType(), True),
		StructField("PctUsePubTrans", 		FloatType(), True),
		StructField("PolicCars", 			FloatType(), True),
		StructField("PolicOperBudg",		FloatType(), True),
		StructField("LemasPctPolicOnPatr",  FloatType(), True),
		StructField("LemasGangUnitDeploy",  FloatType(), True),
		StructField("LemasPctOfficDrugUn",  FloatType(), True),
		StructField("PolicBudgPerPop", 		FloatType(), True),
		StructField("ViolentCrimesPerPop", 	FloatType(), True)
	])

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_communities_crime)
		          .load("/home/spark/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))

#colunas= [PolicOperBudg,ViolentCrimesPerPop,population,racepctblack,agePct12t21,pctWWage,state,county,community,communityname]

df = df.select("PolicOperBudg","ViolentCrimesPerPop","population","racepctblack","agePct12t21","pctWWage","state","county","community","communityname","PctPolicWhite")

#limpar null
def isNa(df):
	names =df.schema.names
	for c in names:
		df=df.withColumn(c, (
			F.when(
				F.col(c).contains("?"), None
			).otherwise(
				F.col(c))
			)
		)
		return df

df = isNa(df)

def quest_1(df):
	df = df.select("communityname","PolicOperBudg")
	df.orderBy("PolicOperBUdg", ascending=False).show()

def quest_2(df):
	df = df.select("communityname","ViolentCrimesPerPop")
	df.orderBy("ViolentCrimesPerPop", ascending=False).show()

def quest_3(df):
	df = df.select("communityname","population")
	df.orderBy("population", ascending=False).show()

def quest_4(df):
	df = df.select("racepctblack", "communityname")
	df.orderBy("racepctblack", ascending=False).show()

def quest_5(df):
	df = df.select("pctWWage", "communityname")
	df.orderBY("pctWWage", ascending=False).show()

def quest_6(df):
	df = df.select("agePct12t21", "communityname")
	df.orderBY("agePct12t21", ascending=False).show()

#correlação: interdependência de duas ou mais variáveis.
#stat.corr -> tras a corelação 
def quest_7(df):
	result = df.stat.corr("PolicOperBudg","ViolentCrimesPerPop")
	print(result)

def quest_8(df):
	result = df.stat.corr("PolicOperBudg","PctPolicWhite")
	print(result)

def quest_9(df):
	result = df.stat.corr("PolicOperBudg","population")
	print(result)

def quest_10(df):
	result = df.stat.corr("ViolentCrimesPerPop","population")
	print(result)

def quest_11(df):
	result = df.stat.corr("ViolentCrimesPerPop","pctWWage")
	print(result)

	def question12(df):
	data = df.filter(F.col('ViolentCrimesPerPop')>0)
	greatest_crimeperpopl_community = data.where(F.col('population')>0)\
										  .groupBy(F.col('communityname'))\
										  .agg(F.max(F.col('population') * F.col('ViolentCrimesPerPop') ).alias('crime_per_popl') )\
										  .orderBy(F.col('crime_per_popl').desc())\
										  .limit(10)\
										  .collect()
	for _row in greatest_crimeperpopl_community:
		print("Raça predominante em: ", _row['communityname'].replace('city', ''))
		data = df.filter(F.col('communityname') == _row['communityname'])
		data = data.withColumn('max_pctRace', (
			F.when(
				(F.col('racepctblack') > F.col('racePctWhite')) &
				(F.col('racepctblack') > F.col('racePctAsian')) &
				(F.col('racepctblack') > F.col('racePctHisp')), 'Black'
			).when(
				(F.col('racePctWhite') > F.col('racepctblack')) &
				(F.col('racePctWhite') > F.col('racePctAsian')) &
				(F.col('racePctWhite') > F.col('racePctHisp')), 'Caucasian'
			).when(
				(F.col('racePctAsian') > F.col('racepctblack')) &
				(F.col('racePctAsian') > F.col('racePctWhite')) &
				(F.col('racePctAsian') > F.col('racePctHisp')), 'Asian'
			).when(
				(F.col('racePctHisp') > F.col('racepctblack')) &
				(F.col('racePctHisp') > F.col('racePctAsian')) &
				(F.col('racePctHisp') > F.col('racePctWhite')), 'Hisp'
			)
		))
		result = data.groupBy(F.col('max_pctRace'))\
					 .count()\
					 .orderBy(F.col('count').desc())\
					 .limit(1)\
					 .first()['max_pctRace']



#selecionando novas colunas
#colunas= [PolicOperBudg,ViolentCrimesPerPop,population,racepctblack,agePct12t21,pctWWage,state,county,community,communityname,PolicOperBudg,PctPolicWhite]



