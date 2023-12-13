# Credit_Card_Fraud_Detection MLOps Pipeline

#  Introduction
In an increasingly digital and interconnected world, the importance of robust fraud detection systems cannot be overstated. Our MLOps project is dedicated to addressing the critical problem of credit card fraud, a pervasive and evolving threat that has significant financial implications for both consumers and financial institutions. The primary focus of this project is to develop, deploy, and continuously manage an advanced machine learning-based fraud detection model. This model will analyze credit card transaction data, distinguishing between legitimate transactions and fraudulent activities with high accuracy. Our goal is to create a comprehensive solution that not only identifies fraudulent transactions but also adapts to shifting patterns and emerging fraud tactics, ensuring the utmost reliability and security for our users. Through diligent monitoring, automated alerts, and iterative model improvement, our MLOps project aims to safeguard financial transactions, instill trust in our services, and minimize the impact of fraudulent activities on both our organization and our valued customers. 

# Project Pipeline
In this project, we have built an ML pipeline in which we train, and deploy an ML model to classify or predict fraudulent credit card transactions. Our pipeline incorporates the following components:

![project steps](https://github.com/HiAditHere/Credit_Card_Fraud_Detection/blob/main/images/Project%20Pipeline%20Graph.png)


  - Data Preprocessing: To clean the data and extract salient features before feeding them to the model
  - Machine Learning Modelling pipeline: To conduct model monitoring and experiment tracking
  - Deployment Pipeline: To deploy the model to an endpoint

# Data Pipeline
Our data pipeline has been orchestrated using Apache Airflow to perform all the cleaning tasks and feature engineering tasks. The raw data and preprocessed data is saved in the GCP cloud Bucket and the data versioning task is taken care by DVC.

Here is an overview of our preprocessing pipeline that runs on airflow

![project steps](https://github.com/HiAditHere/Credit_Card_Fraud_Detection/blob/main/images/Data%20Pipeline%20Graph.png)

Tasks and their functions:
load_data_task:  loads the raw data into a data frame from the cloud 
date_column_task: Performs date transformations on the date time column of our data      frame, extracts new features : month, day, hour
Merchant_column_task: Cleans the merchant column 
Dob_column_task: Extracts the age of each instance from the ‘dob’ column
Distance_task: Computes the distance between the merchant location and the location from which the transaction is made
Ohe_task: One-hot-encodes Gender column
transaction_gap: Calculates the time difference between each card usage in hours
If the card is being used for the firt time, the difference is set to 0 for    that particular   instance
card_frequency_task: Calculates how frequently is the card being used
drop_task: Drops the unwanted columns and reorders the columns of DataFrame
Categorical_columns_task: Encodes Categorical Columns ‘city’, ’job’, ’merchant’, ’category’  using WOE Encoder 
At the end of each task, the resulting dataframe is stored into a pickle file and the same is passed as input to the following task of the dag.

![project steps](https://github.com/HiAditHere/Credit_Card_Fraud_Detection/blob/main/images/dag_image.jpeg)


# Model Development
After preprocessing, we have done model development and experiment tracking using MLFlow. 

![project steps](https://github.com/HiAditHere/Credit_Card_Fraud_Detection/blob/main/images/Model%20Pipeline%20Graph.png)

We have run logistic regression algorithm and RandomForestClassifier algorithm, out of which the RandomForestClassifier has performed well.Using MLFlow, we have logged the parameters ‘n_estimators’, metrics  ‘accuracy’, ‘recall’, and ‘precision’, model artifacts of the various versions of the RandomForestClassifier model. 

![project steps](https://github.com/HiAditHere/Credit_Card_Fraud_Detection/blob/main/images/Deployment%20Pipeline%20Graph.png)


To deploy our model, we have built images for creating the training pipeline and deploying the model. Both these images are registered in the Artifact registry on Google Cloud. These images are later used to build the training pipeline, generate an endpoint on Vertex AI, and deploy the registered model to the endpoint.
Since our data is time series data, we wanted our model to be retrained, for every two months. We have done this using the Airflow dag(retrain.py). The dag is used to process the data of the new window and train the model using the new data. The newly trained model is fetched from the model registry and deployed to the endpoint again.

# Cost Analysis

Scoping

Resources cost: data acquisition costs, software licenses
Time estimation: Time period cost

Development 
	
Data Preprocessing: Data cleaning, Feature engineering, Labeling cost 

Infrastructure cost: cloud resources and computational cost(GPUs for training)

Cloud service charges: 

Software cost: Software installation and license 
	
Deployment 
	
Infrastructure cost: cloud resources, servers and containers  

Cloud service charges: 

Integration cost: application integration

Security cost: security of deployed model 

Monitoring and Maintenance 

Infrastructure cost: cloud resources and maintenance servers  

Model updating: retaining cost with the new data










