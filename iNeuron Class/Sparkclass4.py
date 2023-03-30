from pyspark.sql import SparkSession  # Importing Library for Creating Spark Session
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

from pyspark.sql.functions import col 

if __name__ == "__main__":
    spark = SparkSession.builder.master("spark://localhost:7077").appName("Sparkclass4").getOrCreate() #UrlforSpark Master cluster
    # SparkSession.builder.master("").appName("Sparkclass4").getOrCreate() #UrlforSpark Master cluster  # Localy to that machine 

    #Create list of data to prepare data frame
    person_list = [("Berry","","Allen",1,"M"),
                   ("Oliver","Queen","",2,"M"),
                   ("Robert","","Williams",3,"M"),
                   ("Tony","","Stark",4,"F"),
                   ("Rajiv","Mary","Kumar",5,"F") ]

 #defining schema for dataset
    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", IntegerType(), True), \
        StructField("gender", StringType(), True), \
      
    ])

df = spark.createDataFrame(data=person_list,schema=schema)

df.show(truncate= False)  

df.printSchema() # Printing The Schema 

# Reading the Data From the HDFS PATH 

df1 = spark.read.option("header",True).csv("/input_data/departments.csv")
df1.printSchema()

df2 = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/departments.csv")

df2.printSchema()

df2.show()

empDf = spark.read.option("header",True).option("inferSchema",True).csv("/input_data/employee.csv")
empDf.printSchema()

empDf.show()

empDf.select("*").show() #Showing Complete DataSet

empDf.select("Employee_ID","First_Name").show() # Selecting Particular Column

empDf.select(empDf.EMPLOYEE_ID,empDf.FIRST_NAME).show()

empDf.select(empDf["EMPLOYEE_ID"],empDf["FIRST_NAME"]).show()


# Col Method 

empDf.select(col("Employee_ID"),col("First_Name")).show()


empDf.select(col("Employee_Id").alias("EMP_ID"),col("First_Name").alias("F_NAME")).show()

empDf.select("Employee_ID","First_Name","SALARY").withcolumn("NEW_SALARY,",col("SALARY")+1000).show()


empDf.withcolumn("NEW_SALARY",col("SALARY")+1000).select("Employee_ID","First_Name","NEW_SALARY").show()

empDf.withColumn("SALARY",col("SALARY") - 1000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show()

empDf.withColumnRenamed("SALARY","EMP_SALARY").show() # Rename Column NAME

empDf.drop("COMMISSION_PCT").show() # dROPING THE cOLUMN

empDf.filter(col("SALARY") < 5000).show() ## FILTER THE dATA

empDf.filter(col("SALARY") < 5000).show(100) 

empDf.filter(col("SALARY") < 5000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show(100)

empDf.filter((col("DEPARTMENT_ID") == 50) & (col("SALARY") < 5000)).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)
 
empDf.filter("DEPARTMENT_ID <> 50").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)

empDf.filter("DEPARTMENT_ID != 50").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)

empDf.filter("DEPARTMENT_ID == 50 and SALARY < 5000").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(100)

empDf.distinct().show()

empDf.dropDuplicates().show(100)

empDf.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).show(100)












