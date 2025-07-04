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

## 📎 License

This project is licensed under the MIT License. Feel free to use and modify it for your own data pipeline needs.

---

## 🙌 Author

**Showry Mallavarapu**  
