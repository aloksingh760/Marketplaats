import pyspark.sql.functions as f

def splitFistAndLastName(spark,df,columnName,lastNameIndex=0,firstNameIndex=1):
    """Transform original dataset.
    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    df=df.withColumn("LastName",f.split(f.col(columnName), ",").getItem(lastNameIndex))
    df=df.withColumn("FirstName",f.split(f.col(columnName), ",").getItem(firstNameIndex))
    df=df.drop(columnName)
    return df

def removeSpecialChar(spark,df,columnName):
     """Transform original dataset with remove Special Char during cleaning data.
    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    df=df.withColumn(columnName,f.regexp_replace(f.col(columnName),"[^0-9]",""))
    return df

def removeSpaceFromColumns(spark,df):
     """Transform original dataset with remove Space From Columns during cleaning data.
    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    NewColumns=(column.replace(' ', '') for column in df.columns)
    df=df.toDF(*NewColumns)
    return df