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



hive_database <- Sys.getenv("HIVE_DATABASE")
hive_table.   <- Sys.getenv("HIVE_TABLE")
hive_table_fq <- paste0(hive_database, '.', hive_table)


# -- Set master to "yarn-client" or "local[*]" depending on your deployment type
# To enable Spark push down set  "/etc/spark/conf/spark-defaults.conf" (Ref: https://therinspark.com/tuning.html (Chapter 9.2.2) )
# spark.shuffle.service.port =. 7337 - Fix issue with Spark which default to other value (check in Cloudera Manager if value is correct)
default_config <- list(
  "sparklyr.shell.properties-file" =  "/etc/spark/conf/spark-defaults.conf", 
  "spark.shuffle.service.port"     = "7337"
)

# Connect to Spark
spark <- spark_connect(master="yarn", app_name = "1_data_ingest.R", config = default_config )


# read data into a Spark DataFrame
if (Sys.getenv("STORAGE_MODE") == "external") {
  sql <- paste0('SELECT * FROM ', hive_table_fq)
  telco_data_raw <- sparklyr::sdf_sql(spark, sql)
}

# Feature columns



# data preprocessing and munging

idcol    <- 'customerID'  # ID column


cols <- list (
        c('gender', TRUE),                       # (feature column, Categorical?)
        c('SeniorCitizen', TRUE),
        c('Partner', TRUE),
        c('Dependents', TRUE),
        c('tenure', FALSE),
        c('PhoneService', TRUE),
        c('MultipleLines', TRUE),
        c('InternetService', TRUE),
        c('OnlineSecurity', TRUE),
        c('OnlineBackup', TRUE),
        c('DeviceProtection', TRUE),
        c('TechSupport', TRUE),
        c('StreamingTV', TRUE),
        c('StreamingMovies', TRUE),
        c('Contract', TRUE),
        c('PaperlessBilling', TRUE),
        c('PaymentMethod', TRUE),
        c('MonthlyCharges', FALSE),
        c('TotalCharges', FALSE))


telco_data_raw <- telco_data_raw %>% mutate_all(~ifelse(trimws(.x) == "", NA, .x)) # drop blank rows
telco_data_raw <- telco_data_raw %>%                                               # Change  Yes/No by 1, 0 in LAbel columns
                     mutate(Churn = case_when(
                       Churn == "No"  ~ 0,
                       Churn == "Yes" ~ 1,
                       TRUE ~ Churn
                     ))

telco_data_raw <- telco_data_raw %>%                    # Change 1/0 to Yes/No to match the other binary features
  mutate(SeniorCitizen = case_when(
    SeniorCitizen == 0 ~ "No",
    SeniorCitizen == 1 ~ "Yes",
    TRUE ~ as.character(SeniorCitizen)  
  ))


data   <- telco_data_raw %>% select(-labelcol)                       # separate out the label
labels <- telco_data_raw %>% select( labelcol)

selected_cols <- unlist(lapply(cols, function(x) if (x[2]) x[1]))   # only use the feature columns named in `cols`
data <- data %>% select(selected_cols)


## Machine Learning Model Training
## ---------------------------------

sample_size <- data %>% count()  %>% collect() %>% first() %>% as.numeric()
data_r <- data %>% sample_n(size = sample_size) %>% collect()









