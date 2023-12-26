# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

library(sparklyr)
library(dplyr)
library(stringr)


# Set the setup variables needed by CMLBootstrap
HOST         <- str_split(Sys.getenv("CDSW_API_URL"), ':')[[1]]
HOST         <- paste0 ( HOST[1], "://", Sys.getenv("CDSW_DOMAIN") ) 
USERNAME     <- str_split(Sys.getenv("CDSW_PROJECT_URL"), "/")[[1]][7]  # args.username  # "vdibia"
API_KEY      <- Sys.getenv("CDSW_API_KEY")
PROJECT_NAME <- Sys.getenv("CDSW_PROJECT")

storage      <- Sys.getenv("STORAGE")


# -- Set master to "yarn-client" or "local[*]" depending on your deployment type
# To enable Spark push down set  "/etc/spark/conf/spark-defaults.conf" (Ref: https://therinspark.com/tuning.html (Chapter 9.2.2) )
# spark.shuffle.service.port =. 7337 - Fix issue with Spark which default to other value (check in Cloudera Manager if value is correct)
default_config <- list(
                      "sparklyr.shell.properties-file" =  "/etc/spark/conf/spark-defaults.conf", 
                      "spark.shuffle.service.port"     = "7337"
                  )

default_config
spark <- spark_connect(master="yarn", app_name = "1_data_ingest.R", config = default_config )

print(spark_config(spark))


schema <- list(
        customerID       = "string",
        gender           = "string",
        SeniorCitizen    = "string",
        Partner          = "string",
        Dependents       = "string",
        tenure           = "double",
        PhoneService     = "string",
        MultipleLines    = "string",
        InternetService  = "string",
        OnlineSecurity   = "string",
        OnlineBackup     = "string",
        DeviceProtection = "string",
        TechSupport      = "string",
        StreamingTV      = "string",
        StreamingMovies  = "string",
        Contract         = "string",
        PaperlessBilling = "string",
        PaymentMethod    = "string",
        MonthlyCharges   = "double",
        TotalCharges     = "double",
        Churn            = "string"
)

# Now we can read in the data into Spark
data_location <- Sys.getenv("DATA_LOCATION")


# Raw data stored in CDP Base with 0_bootstrap
# TO BE MODIFY: to use data_location variable
path <- paste0(storage, "/data/churn_prototype/WA_Fn-UseC_-Telco-Customer-Churn-.csv")

telco_data <- spark_read_csv(spark, "mydate", path = path, infer_schema = FALSE, columns = schema, na = "NA",delimiter = ",")

 # ...and inspect the data.
head(telco_data, 10)

sdf_schema(telco_data)

spark.sql("show databases").show()
spark.sql("show tables in " + hive_database).show()

# Create the Hive table, if possible
# This is here to create the table in Hive used be the other parts of the project, if it
# does not already exist.
hive_database <- Sys.getenv("HIVE_DATABASE")
hive_table    <- Sys.getenv("HIVE_TABLE")

hive_table_fq <- paste0(hive_database, ".", hive_table)

spark_write_table(telco_data, name = hive_table_fq )

        
spark_disconnect(spark)
