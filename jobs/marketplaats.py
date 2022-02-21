from dependencies.spark import startSpark
from util.utils import splitFistAndLastName,removeSpaceFromColumns
from pytranse.read import read_csv
from pytranse.write import write_csv
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
#from pyspark.sql.functions import input_file_name,regexp_extract,col,udf,date_format,to_date,count,split,lit,when
from pyspark.sql.types import DateType,StringType,ArrayType,IntegerType
from datetime import datetime,date
import pyspark.sql.functions as f
import ast
import argparse

func =  f.udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), DateType())
make_list_udf = f.udf(lambda x:ast.literal_eval(str(x)), ArrayType(IntegerType()))

parser = argparse.ArgumentParser()
parser.add_argument("--credit_fpath", help="credit limit csv file path")
parser.add_argument("--country_fpath", help="countries csv file path")
parser.add_argument("--usersituation_fpath", help="countries csv file path")
parser.add_argument("--output_fpath", help="countries csv file path")
args = parser.parse_args()
if args.credit_fpath:
    credit_fpath = args.credit_fpath
if args.country_fpath:
    country_fpath = args.country_fpath
if args.usersituation_fpath:
    usersituation_fpath = args.usersituation_fpath
if args.usersituation_fpath:
    output_fpath = args.output_fpath


# start Spark application and get Spark session, logger and config
spark = startSpark(
    appName='Marketplaats')

# load credit_limit_events csv file to spark dataframe
CreditLimitDf=read_csv(spark,credit_fpath,escape='"')

# load countries csv file to spark dataframe
CountriesDf=read_csv(spark,country_fpath)

# load user_situation csv file to spark dataframe
UserSituationDf = read_csv(spark,usersituation_fpath)


# clean and tranform credit_limit_events dataframe
CreditLimitDf=removeSpaceFromColumns(spark,CreditLimitDf)
CreditLimitDf=splitFistAndLastName(spark,CreditLimitDf,"Name")
CreditLimitDf=CreditLimitDf.withColumn("UserSituationIdsstring",f.regexp_replace(f.col("UserSituationIds"),'"',''))
CreditLimitDf=CreditLimitDf.withColumn("UserSituationList",make_list_udf(f.col("UserSituationIdsstring")))
CreditLimitDf1=CreditLimitDf.withColumn("UserSituationList",f.explode(f.col("UserSituationList")))

# join user situation dataframe to credit limit events dataframe
UserSituationDf=removeSpaceFromColumns(spark,UserSituationDf)
UserSituationDf=CreditLimitDf1.join(UserSituationDf ,CreditLimitDf1.UserSituationList == UserSituationDf.SituationId, "left")
UserSituationDf=UserSituationDf.groupby("UserId","CreditLimit").agg(f.collect_list("Name").alias('UserSituation'))
UserSituationDf=UserSituationDf.withColumn("UserSituationNames",f.concat_ws(",",col("UserSituation")))
UserSituationDf=UserSituationDf.drop("UserSituation")
CreditLimitWithSituationDf=CreditLimitDf.join(UserSituationDf,['UserId','CreditLimit'],"left").orderBy('UserId')

# join country dataframe to credit limit events dataframe
CountriesDf=removeSpaceFromColumns(spark,CountriesDf)

CreditSituationCountriesDf=CreditLimitWithSituationDf.join(CountriesDf,["CountryId"],"left")
LatestCreditLimitJoinedDf=CreditSituationCountriesDf.groupBy("UserId").agg(f.max("EventTimestamp").alias("EventTimestamp"))
latestCreditSituationCountriesDf=CreditSituationCountriesDf.join(LatestCreditLimitJoinedDf,["UserId","EventTimestamp"]).orderBy("UserId")
latestCreditSituationCountriesDf=latestCreditSituationCountriesDf.drop("UserSituationList","UserSituationIdsstring")
write_csv(latestCreditSituationCountriesDf,spark,output_fpath,single_file=True)

# log the success and terminate Spark application
spark.stop()