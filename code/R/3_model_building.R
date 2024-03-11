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
library(broom)


hive_database <- Sys.getenv("HIVE_DATABASE")
hive_table.   <- Sys.getenv("HIVE_TABLE")
hive_table_fq <- paste0(hive_database, '.', hive_table)


# -- Set master to "yarn-client" or "local[*]" depending on your deployment type
# To enable Spark push down set  "/etc/spark/conf/spark-defaults.conf" (Ref: https://therinspark.com/tuning.html (Chapter 9.2.2) )
# 7447
# spark.shuffle.service.port =. 7337 - Fix issue with Spark which default to other value (check in Cloudera Manager if value is correct)
default_config <- list(
  "sparklyr.shell.properties-file" =  "/etc/spark/conf/spark-defaults.conf", 
  "spark.shuffle.service.port"     = "7337",
  "spark.ui.port"= "30820"
)

# Connect to Spark
spark <- spark_connect(master="yarn", app_name = "1_data_ingest.R", config = default_config )

sql = 'SELECT 
        customerID, gender, SeniorCitizen, Partner, Dependents,
        CASE 
          WHEN (tenure <= 10)                        THEN "10"
          WHEN (tenure >  10)   AND (tenure <= 20)   THEN "20"
          WHEN (tenure >  20)   AND (tenure <= 30)   THEN "30"
          WHEN (tenure >  30)   AND (tenure <= 40)   THEN "40"
          WHEN (tenure >  40)   AND (tenure <= 50)   THEN "50"
          WHEN (tenure >  50)   AND (tenure <= 60)   THEN "60"
          WHEN (tenure >  60)   AND (tenure <= 70)   THEN "70"
          WHEN (tenure >  70)   AND (tenure <= 80)   THEN "80"
          WHEN (tenure >  80)   AND (tenure <= 90)   THEN "90"
         ELSE "100"
        END AS tenure, 
        PhoneService, MultipleLines, InternetService,
        OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies, Contract,
        PaperlessBilling, PaymentMethod,
        CASE 
          WHEN (MonthlyCharges <= 20)                              THEN "02"
          WHEN (MonthlyCharges >  20)  AND (MonthlyCharges <= 40)  THEN "04"
          WHEN (MonthlyCharges >  40)  AND (MonthlyCharges <= 60)  THEN "06"
          WHEN (MonthlyCharges >  60)  AND (MonthlyCharges <= 80)  THEN "08"
          WHEN (MonthlyCharges >  80)  AND (MonthlyCharges <= 100) THEN "10"
          WHEN (MonthlyCharges >  100) AND (MonthlyCharges <= 120) THEN "12"
         ELSE "14"
         END AS MonthlyCharges,
       CASE 
          WHEN (TotalCharges <= 100)                               THEN "0100"
          WHEN (TotalCharges >  100)   AND (TotalCharges <= 500)   THEN "0500"
          WHEN (TotalCharges >  500)   AND (TotalCharges <= 1000)  THEN "1000"
          WHEN (TotalCharges >  1000)  AND (TotalCharges <= 2000)  THEN "2000"
          WHEN (TotalCharges >  2000)  AND (TotalCharges <= 3000)  THEN "3000"
          WHEN (TotalCharges >  3000)  AND (TotalCharges <= 4000)  THEN "4000"
          WHEN (TotalCharges >  4000)  AND (TotalCharges <= 5000)  THEN "5000"
          WHEN (TotalCharges >  5000)  AND (TotalCharges <= 6000)  THEN "6000"
          WHEN (TotalCharges >  6000)  AND (TotalCharges <= 7000)  THEN "7000"
          WHEN (TotalCharges >  7000)  AND (TotalCharges <= 8000)  THEN "8000"
         ELSE "9000"
         END AS TotalCharges,
         Churn
FROM '

# read data into a Spark DataFrame
if (Sys.getenv("STORAGE_MODE") == "external") {
  sql <- paste0(sql, hive_table_fq)
  telco_data_raw <- sparklyr::sdf_sql(spark, sql)
}


# data preprocessing and munging
# ---------------------------------------------------
#idcol    <- 'customerID'  # ID column

# Feature columns
cols <- list (
        c('gender', TRUE),                       # (feature column, Categorical?)
        c('tenure', TRUE),
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
        c('MonthlyCharges', TRUE),
        c('TotalCharges', TRUE),
        c('Churn', TRUE))


telco_data_raw <- telco_data_raw %>% mutate_all(~ifelse(trimws(.x) == "", NA, .x)) # Replace blank fields by NA
telco_data_raw <- na.omit(telco_data_raw )                                         # Remove null rows
telco_data_raw <- telco_data_raw %>%                                               # Change  Yes/No by 1, 0 in LAbel columns
                     mutate(Churn = case_when(
                       Churn == "No"  ~ 0,
                       Churn == "Yes" ~ 1,
                       TRUE ~ as.integer(Churn)
                     ))

telco_data_raw <- telco_data_raw %>%                    # Change 1/0 to Yes/No to match the other binary features
  mutate(SeniorCitizen = case_when(
    SeniorCitizen == 0 ~ "No",
    SeniorCitizen == 1 ~ "Yes",
    TRUE ~ as.character(SeniorCitizen)  
  ))


selected_cols    <- unlist(lapply(cols, function(x) if (x[2]) x[1]))   # only use the feature columns named in `cols`
telco_data_raw   <- telco_data_raw %>% select(all_of(selected_cols))



glimpse(telco_data_raw)
sdf_schema(telco_data_raw)

## Machine Learning Model Training
## ---------------------------------
# 1: R Logistic regression

sample_size      <- telco_data_raw %>% count()  %>% collect() %>% first() %>% as.numeric()  # Sample Sparlyr dataframe to run on R local
telco_data_raw_r <- telco_data_raw %>% sample_n(size = sample_size) %>% collect()           # Collect Sparlyr to R

n = nrow(telco_data_raw_r)
telco_data_raw_r <- telco_data_raw_r %>% mutate(simple = sample(x = c("train", "test"),     # Sample Train, Test
                                                                size = n,
                                                                replace = TRUE,
                                                                prob = c(0.7, 0.3)))

#s=sample_n(telco_data_raw_r, n, replace = FALSE, prob = c(0.7, 0.3) )

train <- telco_data_raw_r[telco_data_raw_r$simple == "train", ]
test  <- telco_data_raw_r[telco_data_raw_r$simple == "test", ]

nrow(train)    # Check numbers (%70)
nrow(test)     # 20%


#library(mlflow)                  # Start experiment

#mlflow_set_tracking_uri("cml://localhost")
#mlflow_create_experiment("Churn logistic regression with R")
#mlflow_start_run()

model <- glm(Churn ~ gender + tenure + TotalCharges + Contract, 
             data = train, family = binomial) # Issue in sampling (hould be train dataset)

predictions <- stats::predict(model, test, type = "response")


saveRDS(model, "model.rds")

## Show predictions
## ---------------------------------

library(pROC)
auc <- auc(test$Churn, predictions)
auc

roc_curve <- roc(test$Churn, predictions)          # Plot roc curve

plot(roc_curve, main = "ROC", col = "blue", lwd = 2)



#mlflow_log_metric("auc", auc)
#mlflow_save_model(model, "modele_regression_logistique")
#mlflow_end_run()


# 2: Sparlyr Multi-nomial Logistic regression

partitions <- telco_data_raw %>% sdf_random_split(training = 0.7, test = 0.3, seed = 1111)


train <- telco_data_raw %>% sparklyr::sdf_sample(fraction=0.7, replacement=FALSE)
test  <- telco_data_raw %>% sparklyr::sdf_sample(fraction=0.3, replacement=FALSE)

class(train)
glimpse(train)

model <-train %>% 
            ml_logistic_regression(Churn ~ gender + tenure + TotalCharges + Contract)

model
train

pred <- ml_predict(model, test)

ml_binary_classification_evaluator(pred)

coefficients <- tidy(model)

coefficients 

spark_disconnect(spark)



