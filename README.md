# Practice Airflow

This project demonstrates an end-to-end **layered ETL pipeline** orchestrated by Apache Airflow.  
Data is extracted from_pkt **AWS DynamoDB**, processed through multiple **AWS S3 layers**, and finally loaded into **PostgreSQL**.

---

## 📌 Architecture Overview

The architecture follows a structured multi-layer design:

- **Source Layer** → AWS DynamoDB  
- **Cleansed Layer** → AWS S3  
- **Transform Layer** → AWS S3  
- **Target Layer** → PostgreSQL  

### 🔁 Data Flow

DynamoDB → S3 (Cleansed) → S3 (Transformed) → PostgreSQL

---

## 🎯 Project Objectives

The goal of this project is to practice:

- Designing production-like ETL workflows  
- Orchestrating data pipelines using Airflow DAGs  
- Working with AWS services in a layered architecture  

---

## 🔎 Detailed ETL Process

The ETL workflow consists of **2 main steps**:

### Step 1 – Extract & Load to Cleansed Layer
- Extract raw data from DynamoDB  
- Load the extracted data into the **Cleansed** folder in AWS S3  

### Step 2 – Transform & Load to Target
- Extract cleansed data from S3  
- Perform basic transformations  
- Load transformed data into:
  - S3 (Transformed layer)
  - PostgreSQL database  

---

## 🚀 Demo Guide

## 1️⃣ Source Data Overview

Below is a snapshot of the original dataset:

<img width="1565" height="986" alt="image" src="https://github.com/user-attachments/assets/9cd3e588-fb07-4dc6-8a17-4751a37214d5" />

### 📊 Data Schema

| Column | Description | Data Type |
|--------|--------------|------------|
| Order_ID | Order identifier | String |
| Product_ID | Product identifier | String |
| Customer_ID | Customer identifier | String |
| Order_date | Order date | Date |
| Quantity | Quantity ordered | Integer |
| Unit_price | Unit price | Double |
| Total_price | Total amount | Double |
| Shipping_address | Customer shipping address | String |
| Payment_method | Payment method | String |
| Order_status | Order status | String |
| Shipping_method | Shipping method | String |

---

## 2️⃣ Run Step 1 DAG

### Access Airflow

- Open the **Airflow UI**
- You will see two DAGs corresponding to the two workflow steps
- Select the DAG responsible for **Step 1**

<img width="1418" height="665" alt="image" src="https://github.com/user-attachments/assets/0eedd8f9-7853-4d02-adf3-dc863bfb285d" />

---

### Trigger the DAG

Click the **Trigger** button to initiate execution.

<img width="1365" height="640" alt="image" src="https://github.com/user-attachments/assets/2b28b6f2-f8ed-4099-a287-7edb6dfac056" />

---

### Provide Configuration

After clicking **Trigger**, a configuration window will appear:

1. Enter the following value:
   ```
   order_04
   ```
2. Click the blue **Trigger** button  
3. Wait for the migration process to complete  

<img width="1402" height="655" alt="image" src="https://github.com/user-attachments/assets/8cf9861a-834f-4f28-89b7-b9bc63960e4e" />

---

### Successful Execution

Once the DAG completes (as shown below), **Step 1 has been successfully executed**.

<img width="1919" height="894" alt="image" src="https://github.com/user-attachments/assets/560da396-4a08-4b88-9bac-15b6840662f9" />

---

## 3️⃣ Validate the Result in AWS S3

To confirm the process worked correctly:

- Navigate to AWS S3  
- Open the **cleansed** folder  
- Verify that `order_04` is present  

<img width="1539" height="688" alt="image" src="https://github.com/user-attachments/assets/ff3ee412-975d-42da-a491-744c1eecc8bd" />

You can also download the `order_04` CSV file from S3 to validate that the data extraction process was performed accurately.

<img width="1520" height="776" alt="image" src="https://github.com/user-attachments/assets/39b5d45f-191d-44fe-ab9f-bb851537d106" />

---

## 4️⃣ Run Step 2 DAG

After successfully completing **Step 1**, proceed to **Step 2**.

In this step, the pipeline will:

- Extract cleansed data from AWS S3  
- Perform transformation logic  
- Load the transformed data into:
  - The **transformed** folder in S3  
  - The **PostgreSQL** database  

---

### Trigger Step 2

To execute Step 2, repeat the same process in the Airflow UI as in Step 1:

1. Open the Airflow UI  
2. Select the DAG responsible for **Step 2**  
3. Click the **Trigger** button  
4. Provide the required configuration (e.g., `order_04`)  
5. Start the execution and wait for the DAG to complete  

---

### Verify Transformed Data in S3
<img width="1919" height="896" alt="image" src="https://github.com/user-attachments/assets/55ff85aa-9d51-4a7e-a703-5a730f2074b3" />

Once the Step 2 DAG completes successfully, navigate to the **transformed** folder in AWS S3 to verify that `order_04` exists.

<img width="1327" height="620" alt="image" src="https://github.com/user-attachments/assets/2eb35e18-f6f9-4c09-bef7-0d08c8f63d37" />

---

### Download and Validate the Output File

Similar to Step 1, you can download the `order_04` CSV file from the transformed folder to verify that the transformation process was executed correctly.

<img width="1639" height="756" alt="image" src="https://github.com/user-attachments/assets/9f0f07dd-55aa-487c-b733-8859d4f3d2f5" />

---

## 5️⃣ Validate Data in PostgreSQL

To verify the data loaded into PostgreSQL, you can use **DBeaver** to inspect the database tables.

<img width="1919" height="1020" alt="image" src="https://github.com/user-attachments/assets/08dc5d0e-8e61-46df-b68b-3e639d41b913" />

---

## 6️⃣ Transformation Logic

In this project, two basic transformation operations were implemented.

### 1️⃣ Split Shipping Address into Multiple Columns

The `Shipping_address` column is split into three separate columns:

- `Shipping_street`
- `Shipping_city`
- `Shipping_country`

#### Example

**Before Transformation**

| Shipping_address |
|------------------|
| 548 Oxford St, New York, USA |

**After Transformation**

| Shipping_street | Shipping_city | Shipping_country |
|----------------|--------------|------------------|
| 548 Oxford St | New York | USA |

---

### 2️⃣ Add Large Order Flag

If a record has a `Total_price` greater than **1000 USD**, a new flag column named `Is_large_order` will be added.

- `TRUE`  → if `Total_price` > 1000  
- `FALSE` → otherwise  

#### Example

**Before Transformation**

| Total_price |
|-------------|
| 446.59 |
| 1494.15 |

**After Transformation**

| Total_price | Is_large_order |
|-------------|----------------|
| 446.59 | FALSE |
| 1494.15 | TRUE |
