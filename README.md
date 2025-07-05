# 📦 Incremental ETL Load in SQL Server

This project demonstrates an **Incremental ETL (Extract, Transform, Load)** process using **T-SQL** in SQL Server. It uses a watermarking technique to load only new or updated records from a source table into a data warehouse (DWH) table.

---

## 📋 Overview

This solution copies data from the `Orders` table (source) to the `DWH_Orders` table (destination) based on the `LastModifiedDate`. It uses a watermark stored in the `ETL_Watermark` table to keep track of the last load time and ensures that only **incremental changes** are loaded on each run.

---

## 🗃️ Tables

### ✅ `Orders`
The source table containing order data.

```sql
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderAmount DECIMAL(10, 2),
    LastModifiedDate DATETIME
);
```

### ✅ `DWH_Orders`
The destination table that acts as the data warehouse for storing incremental changes.

```sql
CREATE TABLE DWH_Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderAmount DECIMAL(10, 2),
    LastModifiedDate DATETIME
);
```

### ✅ `ETL_Watermark`
This table stores the watermark value for each source table to enable incremental loading.

```sql
CREATE TABLE ETL_Watermark (
    TableName NVARCHAR(100) PRIMARY KEY,
    LastLoadedValue DATETIME
);
```

**Initial Seed Example:**

```sql
INSERT INTO ETL_Watermark (TableName, LastLoadedValue)
VALUES ('Orders', '2000-01-01'); -- initial default date
```

---

## 🔁 Stored Procedure: `Load_Orders_Incrementally`

This stored procedure performs the incremental load from `Orders` to `DWH_Orders` using the last loaded watermark.

### 🧱 Key Steps

1. **Read watermark** from `ETL_Watermark`.
2. **Select new or updated records** from `Orders` using `LastModifiedDate`.
3. **Merge** the changes into `DWH_Orders`.
4. **Update the watermark** with the maximum `LastModifiedDate` loaded.
5. **Clean up** temporary data.

### 📜 Procedure Code

```sql
CREATE PROCEDURE Load_Orders_Incrementally
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastWatermark DATETIME;
    DECLARE @NewWatermark DATETIME;

    SELECT @LastWatermark = LastLoadedValue
    FROM ETL_Watermark
    WHERE TableName = 'Orders';

    IF @LastWatermark IS NULL
        SET @LastWatermark = '2000-01-01';

    SELECT *
    INTO #NewOrUpdatedOrders
    FROM Orders
    WHERE LastModifiedDate > @LastWatermark;

    MERGE DWH_Orders AS Target
    USING #NewOrUpdatedOrders AS Source
    ON Target.OrderID = Source.OrderID
    WHEN MATCHED THEN
        UPDATE SET
            Target.CustomerID = Source.CustomerID,
            Target.OrderAmount = Source.OrderAmount,
            Target.LastModifiedDate = Source.LastModifiedDate
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (OrderID, CustomerID, OrderAmount, LastModifiedDate)
        VALUES (Source.OrderID, Source.CustomerID, Source.OrderAmount, Source.LastModifiedDate);

    SELECT @NewWatermark = MAX(LastModifiedDate)
    FROM #NewOrUpdatedOrders;

    IF @NewWatermark IS NOT NULL
    BEGIN
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE TableName = 'Orders')
            UPDATE ETL_Watermark
            SET LastLoadedValue = @NewWatermark
            WHERE TableName = 'Orders';
        ELSE
            INSERT INTO ETL_Watermark (TableName, LastLoadedValue)
            VALUES ('Orders', @NewWatermark);
    END

    DROP TABLE #NewOrUpdatedOrders;
END;
```

---

## ▶ Usage

### Initial Load

```sql
EXEC Load_Orders_Incrementally;
SELECT * FROM DWH_Orders;
```

### Simulate New + Updated Records

```sql
-- Insert a new order
INSERT INTO Orders VALUES (4, 104, 400.00, '2024-02-01');

-- Update an existing order
UPDATE Orders
SET OrderAmount = 550.00,
    LastModifiedDate = '2024-02-02'
WHERE OrderID = 2;

-- Run the incremental ETL again
EXEC Load_Orders_Incrementally;

-- Check result
SELECT * FROM DWH_Orders;
```

---

## 🧠 Concepts Demonstrated

- ✅ Incremental loading using `LastModifiedDate`
- ✅ Watermark tracking via metadata table
- ✅ UPSERT operation using `MERGE`
- ✅ Idempotent and repeatable ETL logic

---

## 📌 Requirements

- SQL Server (any edition supporting `MERGE`)
- Proper indexing on `OrderID` and `LastModifiedDate` for performance

---

# 📦 Incremental ETL Load in Azure Data Factory using the same above tables

This project demonstrates an **Incremental ETL (Extract, Transform, Load)** process using **T-SQL** in SQL Server and Azure Data Factory. It uses a watermarking technique to load only new or updated records from a source table into a data warehouse (DWH) table.

---

## 🧱 Architecture Overview

- **Source Table:** `Orders`
- **Watermark Table:** `ETL_Watermark`
- **Target Table:** `DWH_Orders`
- **ADF Components Used:** Lookup, Copy Data, Stored Procedure


## 🪜 Step-by-Step: Building the ADF Pipeline

### 🔗 Step 1: Create Linked Services

1. **Go to Manage > Linked services > + New**
2. Choose:
   - **Source**: Azure SQL Database (for `Orders`)
   - **Sink**: Azure SQL Database (for `DWH_Orders`)
   - **simply for this project I am creating another table as DWH_Orders in the same database which is *Sales* so one Linked Serive is suffiecient for this project**
3. Configure the Linked Service:
   - Name: `Sales_Database_Linked_service` 
   - Provide server name, database, authentication method
   - Test connection → Create

![image](https://github.com/user-attachments/assets/04d30e07-4249-4b55-8ba9-a2d641fceb22)

---

### 📂 Step 2: Create Datasets

1. **Go to Author > Datasets > + New Dataset**
2. Select **Azure SQL Database**, linked to:
   - `Sales_Database_Linked_service` for `Orders`
   - `Sales_Database_Linked_service` for `DWH_Orders`
   - `Sales_Database_Linked_service` for `ETL_Watermark`
3. **Configure Three datasets**:
   - Dataset for source Table: `Orders` as name it as `Source_Table`
   - Dataset for target Table: `DWH_Orders` and name it as `Destination_Table`
   - Dataset for WaterMark Table: `ETL_Watermark` and name it as `WaterMark`
   - Select the table name.
  

![image](https://github.com/user-attachments/assets/1f1bab12-869b-470d-8e39-e4c4a25521d0)


---

### 🧪 Step 3: Create the Pipeline

1. **Create new pipeline**: `Incremental_Data_Load`

#### 🔍 3.1 Add Lookup: GetLastWatermark

- **Activity Name**: `GetLastWatermark`
- Type: **Lookup**
- Dataset: select the `WaterMark` dataset
- Query:
  ```sql
  SELECT LastLoadedValue FROM ETL_Watermark WHERE TableName = 'Orders'
  ```

![image](https://github.com/user-attachments/assets/15b5ce43-d081-4671-9ee8-74d26f2e8c95)


#### 📥 3.2 Add Copy Data: CopyNewOrders

- **Source**:
  - Dataset: `DS_Orders`
  - Use **Query** option:
    ```sql
    SELECT * FROM Orders
    WHERE LastModifiedDate > '@{activity('GetLastWatermark').output.firstRow.LastLoadedValue}'
    ```
- **Sink**:
  - Dataset: `DS_DWH_Orders`
- **Mapping Tab**:
  - Enable *Mapping* manually
  - Set **OrderID** as the *Upsert Key* in mapping
  - Enable *Allow Upsert* or *Insert + Update*


![image](https://github.com/user-attachments/assets/b3781969-7590-4b22-af29-635cc2b51fb2)

![image](https://github.com/user-attachments/assets/7e140388-d9d7-4ddf-8d0b-e0680efc2998)


#### 🕓 3.3 Add Lookup: GetMaxWaterMark : ater the the data is update/inserted we need to get the watermark so that it needs to be updated in the water mark table

- **Activity Name**: `GetMaxWaterMark`
- Dataset: pointing to `DWH_Orders`
- Query:
  ```sql
  SELECT MAX(LastModifiedDate) AS MaxWatermark FROM DWH_Orders
  ```

![image](https://github.com/user-attachments/assets/454fec4f-cf68-41a9-a000-0556e96d77d0)

#### ⚙️ 3.4 Add Stored Procedure: UpdateWatermark

- **Activity Name**: `UpdateWatermark`
- **Type**: Stored Procedure activity
- **Linked Service**: `Sales_Database_Linked_service` (pointing to your target Azure SQL database which is the same in our case)
- **Stored Procedure Name**: `sp_UpdateWatermark`
- - **Parameters**:
  ```json
  {
    "TableName": "Orders",
    "LastLoadedValue": "@activity('GetMaxModifiedDate').output.firstRow.MaxWatermark"
  }
  ```

- **Stored Procedure Definition** (run this on your Azure SQL DB before using the activity):
```sql
CREATE PROCEDURE sp_UpdateWatermark
    @TableName NVARCHAR(100),
    @LastLoadedValue DATETIME
AS
BEGIN
    IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE TableName = @TableName)
    BEGIN
        UPDATE ETL_Watermark
        SET LastLoadedValue = @LastLoadedValue
        WHERE TableName = @TableName
    END
    ELSE
    BEGIN
        INSERT INTO ETL_Watermark (TableName, LastLoadedValue)
        VALUES (@TableName, @LastLoadedValue)
    END
END
```

---

### 🚦 Step 5: Publish and Trigger

1. Click **Validate** to check for any errors
2. Use the **Debug** button to test the pipeline and verify each step works as expected.
3. Click **Publish All** to deploy your changes.
4. Once verified, click **Add Trigger > Trigger Now** to run it immediately.
5. Optionally, create a **trigger schedule**:
   - Go to the pipeline canvas
   - Click **Add Trigger > New/Edit**
   - Define a schedule (e.g., every 1 hour, daily, etc.)
   - Save and activate the trigger

> ✅ Your pipeline now performs incremental loads using the Watermark pattern on a scheduled or manual basis.



![image](https://github.com/user-attachments/assets/21f3525c-03ab-4b22-8c4e-12739f034b44)

![image](https://github.com/user-attachments/assets/66a820cf-012f-4089-8e95-6967d2584734)



## 📎 License

This project is licensed under the MIT License. Feel free to use and modify it for your own data pipeline needs.

---

## 🙌 Author

**Showry Mallavarapu**  
