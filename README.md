# Data Integration Pipeline with Snowflake and Azure Data Lake Storage

## Overview

This project demonstrates the integration of Snowflake with Azure Data Lake Storage (ADLS) to create a comprehensive data pipeline. It includes setting up Azure and Snowflake accounts, configuring storage, creating containers, and executing SQL commands to manage and transform data.

## Prerequisites

- **Azure Account**: Ensure you have access to Microsoft Azure.
- **Snowflake Account**: Set up a Snowflake account for data processing.
- **SQL Knowledge**: Familiarity with SQL is required to execute the scripts provided.

## Project Setup

### Step 1: Setting Up Azure Data Lake Storage (ADLS)

#### 1\. Create an Azure Account
- Visit the [Azure Portal](https://portal.azure.com) and sign up for an account if you don't already have one.

#### 2\. Create a Storage Account
- Navigate to the Azure Portal dashboard.
- Click on "Create a resource" and select "Storage account".
- Fill in the required fields:
  - **Subscription**: Choose your subscription.
  - **Resource Group**: Create a new resource group or use an existing one.
  - **Storage Account Name**: Choose a unique name for your storage account.
  - **Region**: Select the region closest to your operations.
  - **Performance**: Choose "Standard" or "Premium" based on your needs.
  - **Replication**: Select "Locally-redundant storage (LRS)" for simplicity.
  - **Access tier**: Choose "Hot" for frequent access.
- **Enable Hierarchical Namespace**: Check this option to configure your storage account as ADLS Gen2.

#### 3\. Create Containers
- Once your storage account is created, navigate to it.
- Select "Containers" from the left-hand menu.
- Click "+ Container" to create new containers. Name them as follows:
  - **Customer**
  - **Order**
  - **Product**

### Step 2: Configuring Snowflake

#### 1\. Set Up Snowflake Account
- Log in to your Snowflake account.
- Navigate to the "Worksheets" section to create a new SQL worksheet.

#### 2\. Create Storage Integration
- **Find Tenant ID**: Search for 'Entra ID' in the Azure portal to find your Tenant ID.
- Execute the following SQL command to establish storage integration:
  ```sql
  CREATE OR REPLACE STORAGE INTEGRATION azure_pacificretail_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<Tenant_ID>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<container_name>')
-   Give Consent: Add the Describe command in Snowflake to get the Azure consent link. Click the link, sign in, and give consent.

#### 3\. Assign Permissions in Azure

-   Multi-Tenant App Name: Copy the name of the Azure Multi-Tenant App (before the underscore).
-   Navigate to your storage account in the Azure portal.
-   Select "Access Control (IAM)".
-   Click "Add role assignment" and choose "Storage Blob Data Contributor".
-   Assign this role to the Snowflake application by pasting the application name.

#### 4\. Create External Stage

-   Execute the SQL script: [External_Stage_Creation.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/External_Stage_Creation_edited.sql).
    -   Explanation: This script sets up an external stage in Snowflake to connect with ADLS. It specifies the storage integration and URL to the ADLS container, enabling Snowflake to access data stored in Azure.
    -   Important Note: Now, the stage cannot be created before the database is created. This stage will be created in the next step where we establish our first bronze layer database.

### Step 3: SQL Code Execution

#### 1\. Create Database and Schema

-   Execute: [Create_DB_Bronze_Schema.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Create_DB_Bronze_Schema.sql).
    -   Explanation: This script creates the initial database and schema in Snowflake where raw data will be stored. The bronze schema is used for storing unprocessed data directly from the source.

#### 2\. Load Data into Bronze Layer

-   Execute the following scripts to load data:
    -   [Customer_Load.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Customer_Load.sql)
        -   Explanation: Loads customer data from ADLS into Snowflake's bronze layer, using the CSV format to import data efficiently.
    -   [Product_Load.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Product_load.sql)
        -   Explanation: Loads product data from ADLS, which is stored in JSON format, into the bronze layer, allowing for structured data processing.
    -   [Orders_Load.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Orders_load.sql)
        -   Explanation: Imports transaction data stored in Parquet format into the bronze layer, ensuring efficient handling of large datasets.

#### 3\. Stream Creation

-   Execute: [Stream_Creation.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Stream_Creation.sql).
    -   Explanation: Creates streams in Snowflake to capture changes in the bronze tables. This allows for incremental data loading and ensures that updates are tracked effectively.

### Step 4: Data Transformation Procedures

#### 1\. Transform Customer Data

-   Execute: [Customer_Transform.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Customer_Transform.sql).
    -   Explanation: Applies transformations to clean and standardize customer data, including email validation, age verification, and customer type standardization.

#### 2\. Transform Product Data

-   Execute: [Product_Transform.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Product_Transform.sql).
    -   Explanation: Processes product data by validating prices and stock quantities, ensuring no negative values, and standardizing product ratings.

#### 3\. Transform Orders Data

-   Execute: [Orders_Transform.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Orders_Transform.sql).
    -   Explanation: Filters order data based on transaction validity and ensures only meaningful records are retained for analysis.

### Step 5: Load Data into Silver Layer

-   Execute: [Silver_Data_Load.sql]((https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Silver_Data_Load.sql)).
    -   Explanation: Transfers transformed data into the silver layer, where cleaned and standardized datasets are stored for further analysis.

### Step 6: Create Gold Layer

#### 1\. Create Gold Layer Schema

-   Execute: [Gold_layer.sql]((https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Gold_layer.sql)).
    -   Explanation: Establishes the schema for the gold layer, which is designed for high-level analytics and reporting.

#### 2\. Create Gold Layer Views

-   Execute:
    -   [GoldLayer_View1.sql](https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/GoldLayer_view1.sql)
        -   Explanation: Creates a view for analyzing daily sales data, combining customer, product, and order information for comprehensive insights.
    -   [GoldLayer_View2.sql]((https://github.com/IParkar/Data-Integration-Pipeline-with-Snowflake-and-Azure-Data-Lake-Storage/blob/main/Goladlayer_View2.sql))
        -   Explanation: Develops a customer affinity view to understand purchasing behaviors and product preferences.

### Step 7: Data Files

-   Customer Data: `customer.csv`
-   Product Data: `products.json`
-   Transaction Data: `transaction.snappy.parquet`

Usage
-----

1.  Clone the repository to your local machine.
2.  Follow the setup instructions to configure your Azure and Snowflake accounts.
3.  Execute the SQL scripts in the order specified to replicate the data pipeline.

Conclusion
----------

This project showcases the integration of Snowflake with Azure Data Lake Storage, providing a robust solution for managing and processing large datasets. Feel free to explore and adapt the SQL scripts to fit your specific data requirements.
