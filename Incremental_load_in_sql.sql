use TEST;

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderAmount DECIMAL(10, 2),
    LastModifiedDate DATETIME
);

INSERT INTO Orders VALUES
(1, 101, 500.00, '2024-01-01'),
(2, 102, 300.00, '2024-01-05'),
(3, 103, 250.00, '2024-01-10');

CREATE TABLE DWH_Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderAmount DECIMAL(10, 2),
    LastModifiedDate DATETIME
);

CREATE TABLE ETL_Watermark (
    TableName NVARCHAR(100) PRIMARY KEY,
    LastLoadedValue DATETIME
);

INSERT INTO ETL_Watermark (TableName, LastLoadedValue)
VALUES ('Orders', '2000-01-01'); -- initial value for first load the date value should be lesser than the value in the orders table



CREATE PROCEDURE Load_Orders_Incrementally
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastWatermark DATETIME;
    DECLARE @NewWatermark DATETIME;

    -- 1. Get the last loaded watermark
    SELECT @LastWatermark = LastLoadedValue
    FROM ETL_Watermark
    WHERE TableName = 'Orders';

    -- If the watermark is not set, use a default value
    IF @LastWatermark IS NULL
    BEGIN
        SET @LastWatermark = '2000-01-01';
    END

    -- 2. Load new or updated rows since the last watermark
    SELECT *
    INTO #NewOrUpdatedOrders -- Temporary Table
    FROM Orders
    WHERE LastModifiedDate > @LastWatermark;

    -- 3. Merge into the destination table
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

    -- 4. Update the watermark if any new rows were loaded
    SELECT @NewWatermark = MAX(LastModifiedDate)
    FROM #NewOrUpdatedOrders;

    IF @NewWatermark IS NOT NULL
    BEGIN
        -- If watermark already exists, update it
        IF EXISTS (SELECT 1 FROM ETL_Watermark WHERE TableName = 'Orders')
        BEGIN
            UPDATE ETL_Watermark
            SET LastLoadedValue = @NewWatermark
            WHERE TableName = 'Orders';
        END
        ELSE
        BEGIN
            INSERT INTO ETL_Watermark (TableName, LastLoadedValue)
            VALUES ('Orders', @NewWatermark);
        END
    END

    -- 5. Cleanup temp table
    DROP TABLE #NewOrUpdatedOrders;
END;




exec Load_Orders_Incrementally;


select * from DWH_Orders;


INSERT INTO Orders VALUES (4, 104, 400.00, '2024-02-01');
UPDATE Orders SET OrderAmount = 550.00, LastModifiedDate = '2024-02-02' WHERE OrderID = 2;
