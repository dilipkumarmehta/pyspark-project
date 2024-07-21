



from pyspark.sql.types import StringType,StructField,IntegerType,DateType,StructType

#dbfs:/FileStore/shared_uploads/dilipmehtakk1@gmail.com/menu_csv.txt
#dbfs:/FileStore/shared_uploads/dilipmehtakk1@gmail.com/sales_csv.txt
salsedata="dbfs:/FileStore/shared_uploads/dilipmehtakk1@gmail.com/sales_csv.txt"


schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)

])
salse_df=spark.read.format("csv").option("inferschema","true").schema(schema).load(salsedata)
salse_df.show()


#Adding date month and years from date field
from pyspark.sql.functions import month, year, quarter

salse_df=salse_df.withColumn("order_year",year(salse_df.order_date)).withColumn("order_month",month(salse_df.order_date)).withColumn("order_quarter",quarter(salse_df.order_date))
salse_df.show(100)


#Menu data frame 

from pyspark.sql.types import StructType,StringType,IntegerType,DateType,StructField
menu_data="dbfs:/FileStore/shared_uploads/dilipmehtakk1@gmail.com/menu_csv.txt"
schema=StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True)
])

menu_df=spark.read.format("csv").options().schema(schema).load(menu_data)
menu_df.show()



#Total Amount Spend by Each Customerfrom
total=(salse_df.join(menu_df,'product_id').groupBy('customer_id').agg({'price':'sum'}).orderBy('customer_id')
       
       )
display(total)

#Total Amount Spend by Each each food category
total=(salse_df.join(menu_df,'product_id').groupBy('product_name').agg({'price':'sum'}).orderBy('product_name')
       
       )
display(total)

#Total Amount of sales in each month
total=(salse_df.join(menu_df,'product_id').groupBy('order_month').agg({'price':'sum'}).orderBy('order_month')
       
       )
display(total)

#Tota year sales 
total=(salse_df.join(menu_df,'product_id').groupBy('order_year').agg({'price':'sum'}).orderBy('order_year')
       
       )
display(total)
