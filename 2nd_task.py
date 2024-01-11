from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import col,sum,flatten
from pyspark.sql.functions import arrays_zip, explode


# import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, LongType, \
    DoubleType

from pyspark.sql.functions import posexplode

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()


countries = spark.read.json('countries.json')
events = spark.read.json('events.json')
membership = spark.read.json('memberships.json')
organizations = spark.read.json('organizations.json')
persons = spark.read.json('persons.json')
areas = spark.read.json('areas.json')

# persons.show()
# membership.show()
# countries.show()
# events.show()
# organizations.show()
# areas.show(truncate=False)

# Rename id column to org_id and name to org_name from organizations_json file

org_change = organizations.withColumnRenamed("id","org_id").withColumnRenamed("name","org_name").withColumnRenamed('image','org_image').\
    withColumnRenamed('identifiers','org_identifiers').withColumnRenamed('other_names','org_other_names').withColumnRenamed('type','org_type')

events_change = events.withColumnRenamed('identifiers','event_identifiers')

#Join persons.json and memberships.json based on id and person_id
person_change =persons.withColumnRenamed('links','person_links').withColumnRenamed('id','person_id').withColumnRenamed('image','person_image').\
    withColumnRenamed('identifiers','person_identifiers').withColumnRenamed('other_names','person_other_names').withColumnRenamed('type','person_type')
per_mem = person_change.join(membership,person_change.person_id==membership.person_id)


# renaming area name to area_name for filter
area_name_change = areas.withColumnRenamed("name","area_name").withColumnRenamed('id','area_id')

# area_filter =areas.filter(areas.name.isin('Texas', 'Alabama', 'California', 'New York', 'Arizona' ,'Georgia'))


# Join the data with area file based on area_id

per_mem_area = per_mem.join(area_name_change,area_name_change.area_id==per_mem.area_id)

#Filter data only for states “Texas”, “Alabama”, “California”, “New York”, “Arizona” and “Georgia”


area_filter_new =per_mem_area.filter(per_mem_area.area_name.isin('Texas', 'Alabama', 'California', 'New York', 'Arizona' ,'Georgia'))


#Join the result of item-4 with organizations.json - key would be org_id and organization_id

per_mem_area_org = area_filter_new.join(org_change,org_change.org_id == area_filter_new.organization_id)

#drop the redundant fields :- person_id and org_id

per_mem_area_org_drop_column=per_mem_area_org.drop('person_id','org_id')


per_mem_area_org_drop_column.printSchema()

# cols = len(per_mem_area_org_drop_column.columns)
# print("count before flatten",cols)
# print("====================================================================")

#Combine and Flatten the final result data set from json and store into a single csv file alone


df_select =  per_mem_area_org_drop_column.select('birth_date','death_date','family_name','gender','given_name','person_image','name',
                                                'sort_name','end_date','legislative_period_id','on_behalf_of_id','organization_id',
                                                      'role','start_date','area_name','type','classification','org_image','org_name',
                                                      'seats','org_type',explode(arrays_zip('org_identifiers','person_other_names',
                                                    'person_links','identifiers','other_names','contact_details','person_identifiers')))

exp_step2_df = df_select.select('birth_date','death_date','family_name','gender','given_name','person_image','name',
                                'sort_name','end_date','legislative_period_id','on_behalf_of_id','organization_id',
                                 'role','start_date','area_name','type','classification','org_image','org_name',
                                'seats','org_type',"col.contact_details.*","col.person_identifiers.*","col.other_names.*",
                                "col.identifiers.*","col.person_links.*","col.person_other_names.*",
                                "col.org_identifiers.*")


exp_step2_df.show()













