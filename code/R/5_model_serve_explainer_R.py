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
import os
import sys
from cml import metrics_v1

#https://docs.cloudera.com/machine-learning/1.5.3/runtimes/topics/ml-models-in-pbj-workbench.html


os.environ['RETICULATE_PYTHON'] = sys.executable
os.environ['R_HOME'] = '/usr/lib/R'

import rpy2.robjects as robjects

r = robjects.r

model = r.readRDS(file='model.rds')


#Load the R model saved earlier.

#json_input={
#"gender"           : "Female",
  "tenure"           : "20",
  "PhoneService"     : "Yes",
  "MultipleLines"    : "No",
  "InternetService"  : "DSL",
  "OnlineSecurity"   : "No",
  "OnlineBackup"     : "No",
  "DeviceProtection" : "No",
  "TechSupport"      : "No",
  "StreamingTV"      : "No",
  "StreamingMovies"  : "No",
  "Contract"         : "Month-to-month",
  "PaperlessBilling" : "No",
  "PaymentMethod"    : "Bank transfer (automatic)",
  "MonthlyCharges"   : "06",
  "TotalCharges"     : "1000"
}


def f_predict(json_input):
   # Track inputs
   metrics_v1.track_metric('input_data', json_input)
   # Predict
   rdf = robjects.DataFrame(json_input)
   pred = r.predict(model, rdf, type = "response")
   # Track our prediction
   metrics_v1.track_metric('prediction', pred[0])
   # Return Input and prediction
   return { "data" : json_input, "prediction" : pred[0]}



#print (f_predict(json_input))


