from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ConfirmGrowRateAnalysis").getOrCreate()
# coding: utf-8

# In[10]:


header_text = "source,isTrueDirect,sourceKeyword,medium,isVideoAd,fullVisitorId,visitId,date,newVisits,hitReferer,hitType,hitAction_type,hitNumber,hitHour,hitMin,timeMicroSec,v2ProductName,productListName,isClick,isImpression,sessionQualityDim,timeOnScreen,timeOnSite,totalTransactionRevenue"


# In[11]:


from pyspark.sql.types import *


# In[12]:


attr_list = []


# In[13]:


for each_attr in header_text.split(','):
    attr_list.append(StructField(each_attr, StringType(), True))


# In[14]:


custom_schema = StructType(attr_list)


# In[15]:


custom_schema


# In[16]:


raw_df = spark.read.format('csv').schema(custom_schema).option('header','true').option('mode','DROPMALFORMED').load('gs://aekanunlab/funnel/*')


# In[17]:


raw_df.count()


# In[18]:


from pyspark.sql import functions as sparkf


# In[19]:


mem_raw_df = raw_df.repartition(60).cache()


# In[20]:


allCol_list = mem_raw_df.columns


# In[21]:


removeCol_list = ['timeOnScreen']


# In[22]:


for i in removeCol_list:
    allCol_list.remove(i)


# In[23]:


newCol_list = []


# In[24]:


newCol_list = allCol_list


# In[25]:


filtered_raw_df = mem_raw_df.select(newCol_list)


# In[26]:


from pyspark.sql.functions import udf
from pyspark.sql.types import *


# In[27]:


def f_removenull(origin):
    if origin == None:
        return 'NULL'
    else:
        return origin


# In[28]:


removenull = udf(lambda x: f_removenull(x),StringType())


# In[29]:


def f_makebinary(origin):
    if origin == None:
        return 'FALSE'
    elif origin == 'true':
        return 'TRUE'
    elif origin == '1':
        return 'TRUE'
    else:
        return 'NULL'


# In[30]:


makebinary = udf(lambda x: f_makebinary(x),StringType())


# In[31]:


def f_cleanNullwithZero(item):
    if item == None:
        new = '0'
        return new
    else:
        return item


# In[32]:


cleanNullwithZero = udf(lambda x:f_cleanNullwithZero(x),StringType())


# In[33]:


def f_makedollar(revenue):
    if revenue == None:
        return 0
    else:
        return revenue/1000000


# In[34]:


makedollar = udf(lambda x: f_makedollar(x),FloatType())


# In[35]:


from pyspark.sql.functions import col


# In[36]:


crunched_df = filtered_raw_df.withColumn('hitHour',col('hitHour').cast(FloatType())).withColumn('hitMin',col('hitMin').cast(FloatType())).withColumn('hitNumber',col('hitNumber').cast(FloatType())).withColumn('timeMicroSec',col('timeMicroSec').cast(FloatType())).withColumn('timeOnSite',col('timeOnSite').cast(FloatType())).withColumn('totalTransactionRevenue',cleanNullwithZero(col('totalTransactionRevenue')).cast(FloatType())).withColumn('newVisits',makebinary(col('newVisits'))).withColumn('sourceKeyword',removenull(col('sourceKeyword'))).withColumn('isVideoAd',makebinary(col('isVideoAd'))).withColumn('hitReferer',removenull(col('hitReferer'))).withColumn('isClick',makebinary(col('isClick'))).withColumn('isImpression',makebinary(col('isImpression'))).withColumn('sessionQualityDim',removenull(col('sessionQualityDim'))).withColumn('timeOnSite',removenull(col('timeOnSite'))).withColumn('totalTransactionRevenue',makedollar(col('totalTransactionRevenue'))).withColumn('isTrueDirect',makebinary(col('isTrueDirect')))


# In[37]:


from pyspark.sql.functions import col, udf, sum
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.window import Window


# In[38]:


crunched_df


# In[39]:


crunched_df.columns


# In[40]:


import sys


# In[41]:


from pyspark.sql.types import *


# In[42]:


from pyspark.sql.functions import *


# In[43]:


w = Window()   .partitionBy('fullVisitorId','visitId')   .orderBy(col("hitNumber").cast("long"))


# In[44]:


windowSpec = Window.partitionBy('fullVisitorId','visitId').orderBy(col("hitNumber").cast("long")).rangeBetween(-sys.maxsize, sys.maxsize)


# In[45]:


from pyspark.sql import functions as func


# In[46]:


Diff_hitNumber = func.max(col('hitNumber')                          .cast(IntegerType())).over(windowSpec) - func.min(col('hitNumber')                                                                            .cast(IntegerType())).over(windowSpec)


# In[47]:


Diff_timeMicroSec = func.max(col('timeMicroSec').cast(IntegerType())).over(windowSpec) - func.min(col('timeMicroSec').cast(IntegerType())).over(windowSpec)


# In[48]:


Diff_hitHour = func.max(col('hitHour').cast(IntegerType())).over(windowSpec) - func.min(col('hitHour').cast(IntegerType())).over(windowSpec)


# In[49]:


Diff_hitMin = func.max(col('hitMin')                       .cast(IntegerType())).over(windowSpec) - func.min(col('hitMin')                                                                         .cast(IntegerType())).over(windowSpec)


# In[50]:


first_hitNumber = func.first(col('hitNumber').cast(IntegerType())).over(windowSpec)


# In[51]:


last_hitNumber = func.last(col('hitNumber').cast(IntegerType())).over(windowSpec)


# In[52]:


first_Action_type = func.first(col('hitAction_type').cast(StringType())).over(windowSpec)


# In[53]:


last_Action_type = func.last(col('hitAction_type').cast(StringType())).over(windowSpec)


# In[54]:


from pyspark.sql.functions import to_timestamp


# In[55]:


partitionCal_df = crunched_df.withColumn('first_hitNumber', first_hitNumber).withColumn('last_hitNumber', last_hitNumber).withColumn('Diff_hitNumber', Diff_hitNumber).withColumn('Diff_timeMicroSec', Diff_timeMicroSec).withColumn('Diff_hitHour', Diff_hitHour).withColumn('Diff_hitMin', Diff_hitMin).withColumn('first_Action_type', first_Action_type).withColumn('last_Action_type', last_Action_type).withColumn('last_Action_type',col('last_Action_type').cast(FloatType())).dropna()


# In[56]:


partitionCal_df.columns


# In[57]:


def f_removeLastItem(list):
    list.pop()
    return list


# In[58]:


removeLastItem = func.udf(lambda x:removeLastItem(x))


# In[59]:


collectList_df = partitionCal_df.groupBy([
'source',
 ##'isTrueDirect',
 ##'sourceKeyword',
 ##'medium',
 'isVideoAd',
 'fullVisitorId',
 'visitId',
 #'date',
 ##'newVisits',
 ##'hitReferer',
 #'hitType',
 #'hitAction_type',
 #'hitNumber',
 #'hitHour',
 #'hitMin',
 #'timeMicroSec',
 #'v2ProductName',
 #'productListName',
 #'isClick',
 #'isImpression',
 ##'sessionQualityDim',
 #'timeOnSite',
 #'totalTransactionRevenue',
 'first_hitNumber',
 'last_hitNumber',
 'Diff_hitNumber',
 'Diff_timeMicroSec',
 'Diff_hitHour',
 'Diff_hitMin',
 'first_Action_type',
 'last_Action_type']).agg(func.collect_list('hitAction_type'))\
#.withColumn('collect_list(hitAction_type)',removeLastItem(col('collect_list(hitAction_type)')))


# In[60]:


funnel_col_list = collectList_df.columns


# In[61]:


raw_funnel_df = collectList_df.select(['fullVisitorId',
 'visitId',
 'first_hitNumber',
 'last_hitNumber',
 'Diff_hitNumber',
 'Diff_hitHour',
 'Diff_hitMin',
 'Diff_timeMicroSec',
 'first_Action_type',
 'last_Action_type',
 'collect_list(hitAction_type)'])


# In[62]:


def f_removedupINLIST(l):
    seen = set()
    new_list = [x for x in l if not (x in seen or seen.add(x))]
    new_list.pop()
    return new_list


# In[63]:


removedupINLIST = func.udf(lambda x: f_removedupINLIST(x))


# In[64]:


f_length_list = func.udf(lambda x: len(x),IntegerType())


# In[65]:


seq_funnel_df = raw_funnel_df.withColumn('seq_hitAction_type',removedupINLIST(col('collect_list(hitAction_type)'))).withColumn('length_hitAction_type',f_length_list(col('collect_list(hitAction_type)')))


# In[66]:


final_df = seq_funnel_df


# In[ ]:


final_df.drop('collect_list(hitAction_type)').write.mode('overwrite').csv('gs://aekanunlab/ABTCustomerBehaviorResult/')

