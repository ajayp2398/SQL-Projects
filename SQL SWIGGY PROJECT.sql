SELECT * FROM Swiggy_Data

--Data Validation & Data Cleaning--
--Null Check--
SELECT 
 SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END ) AS null_state,
 SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_city,
 SUM(CASE WHEN Order_Date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
 SUM(CASE WHEN Restaurant_Name IS NULL THEN 1 ELSE 0 END) AS null_restaurant_name,
 SUM(CASE WHEN Location IS NULL THEN 1 ELSE 0 END) AS null_category,
 SUM(CASE WHEN Dish_Name IS NULL THEN 1 ELSE 0 END) AS null_dish_name,
 SUM(CASE WHEN Price_INR IS NULL THEN 1 ELSE 0 END) AS null_prince_inr,
 SUM(CASE WHEN Rating IS NULL THEN 1 ELSE 0 END) AS null_rating,
 SUM(CASE WHEN Rating_count IS NULL THEN 1 ELSE 0 END) AS null_rating_count
 FROM Swiggy_Data

 --Emplty String--

 SELECT * FROM Swiggy_Data
 WHERE State='' OR City='' OR Restaurant_Name='' OR Category='' OR Dish_Name=''

 --Duplicate Detection--

 SELECT 
  State, City, Order_Date, Restaurant_Name, Location, Category, Dish_Name, 
  Price_INR, Rating, Rating_Count,
  count(*) as CNT 
  From Swiggy_Data 
  GROUP BY 
  State, City, Order_Date, Restaurant_Name, Location, Category, Dish_Name, 
  Price_INR, Rating, Rating_Count
  Having count(*)>1 
  
  --Delete Duplication--

WITH CTE As (
SELECT *, ROW_NUMBER() OVER(
PARTITION BY State, City, Order_Date, Restaurant_Name, Location, Category, Dish_Name,
Price_INR, Rating, Rating_Count 
ORDER BY (SELECT NULL)) As rn
FROM Swiggy_Data)
DELETE CTE WHERE rn>1

--CREATE SCHEMA---
--DIMENSION TABLE--
--DATE TABLE--

CREATE TABLE dim_date(
  date_id INT IDENTITY(1,1) PRIMARY KEY,
  Full_date DATE,
  Year INT,
  Month INT,
  Month_Name VARCHAR(20),
  Quarter INT,
  Day INT,
  Week INT
  )

  SELECT * FROM dim_date

  --Location Table--

  CREATE TABLE dim_location(
   location_id INT IDENTITY(1,1) PRIMARY KEY,
   state VARCHAR(100),
   city VARCHAR(100),
  location VARCHAR(200)
  );

  ---Restaurant Table--

  CREATE TABLE dim_restaurant(
  restaurant_id INT IDENTITY(1,1) PRIMARY KEY,
  restaurant_name VARCHAR(200)
  );

  --Category Table--

  CREATE TABLE dim_category(
  category_id INT IDENTITY(1,1) PRIMARY KEY,
  category_name VARCHAR(200)
  );

  --DISH_NAME TABLE--

  CREATE TABLE dim_dish_name(
  dish_name_id INT IDENTITY(1,1) PRIMARY KEY,
  dish_name VARCHAR(200)
  );

  SELECT * FROM Swiggy_Data
  use Swiggy_data

  --Creating Fact Table--

  CREATE TABLE fact_swiggy_orders(
   order_id INT IDENTITY(1,1) PRIMARY KEY,

   date_id INT,
   price_inr DECIMAL(10,2),
   rating DECIMAL(4,2),
   rating_count INT,

   location_id INT,
   restaurant_id INT,
   category_id INT,
   dish_name_id INT,

   FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
   FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
   FOREIGN KEY (restaurant_id) REFERENCES dim_restaurant(restaurant_id),
   FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
   FOREIGN KEY (dish_name_id) REFERENCES dim_dish_name(dish_name_id)
   );
   use Swiggy_data
   select * from fact_swiggy_orders
   SELECT * FROM dim_date

   --INSERTING DATA--
   --dim_date--
   INSERT INTO dim_date(Full_date,Year, Month, Month_Name, Quarter, Day, Week)
   SELECT DISTINCT
      Order_date,
      YEAR(Order_Date),
      MONTH(Order_Date),
      DATENAME(MONTH, Order_Date),
      DATEPART(Quarter, Order_Date),
      DAY(Order_Date),
      DATEPART(WEEK, Order_Date)
      FROM Swiggy_Data
      WHERE Order_Date IS NOT NULL;

      SELECT * FROM dim_date;

      --dim_location--
      SELECT * FROM dim_location

      INSERT INTO dim_location(state,city,location)
      SELECT DISTINCT 
      State,
      City,
      Location
      FROM Swiggy_Data;

      Select * FROM dim_location;
      
      --INSERT DATA INTO dim_restaurant--
      SELECT * FROM dim_restaurant

      INSERT INTO dim_restaurant(restaurant_name)
      SELECT DISTINCT
      restaurant_name
      FROM Swiggy_Data;

      SELECT * FROM dim_restaurant

      --INSERT DATA INTO dim_category--

      SELECT * FROM dim_category

      INSERT INTO dim_category(category_name)
      SELECT DISTINCT 
       Category
      FROM Swiggy_Data;

      SELECT * FROM dim_category

      --INSERT INTO dim_dish--

      SELECT * FROM dim_dish_name
      SELECT * FROM Swiggy_Data

      INSERT INTO dim_dish_name(dish_name)
      SELECT DISTINCT 
      Dish_Name
      from Swiggy_Data

      SELECT * FROM dim_dish_name;
      use Swiggy_data
      SELECT * FROM fact_swiggy_orders

      --INSERT INTO fact_swiggy_orders--

     INSERT INTO fact_swiggy_orders
     ( date_id,
      price_inr,
      rating,
      rating_count,
      location_id,
      restaurant_id,
      category_id,
      dish_name_id
      )
      SELECT 
       dd.date_id,
       s.Price_INR,
       s.rating,
       s.rating_count,
       dl.location_id,
       dr.restaurant_id,
       dc.category_id,
       dsh.dish_name_id

       FROM Swiggy_Data s 

       Join dim_date dd 
       ON dd.Full_date=s.Order_Date

       JOIN dim_location dl
       ON dl.state=s.State
       AND dl.City=s.City
       AND dl.location=s.location

       JOIN dim_restaurant dr
       ON dr.restaurant_name=s.Restaurant_Name

       JOIN dim_category dc
       ON dc.category_name=s.Category

       JOIN dim_dish_name dsh
       ON dsh.dish_name=s.Dish_Name;

       SELECT * FROM fact_swiggy_orders

       SELECT * FROM fact_swiggy_orders f
       JOIN dim_date d ON d.date_id= f.date_id
       JOIN dim_location l ON l.location_id=f.location_id
       JOIN dim_restaurant r ON r.restaurant_id=f.restaurant_id
       JOIN dim_category c ON c.category_id=f.category_id
       JOIN dim_dish_name dsh ON dsh.dish_name_id=f.dish_name_id

       use Swiggy_data
       
       ---KPIs---

       --Total Orders--

       SELECT COUNT(*) AS TOTAL_ORDERS
       FROM fact_swiggy_orders

       ---TOTAL REVENUE--

       SELECT
       FORMAT(SUM(CONVERT(FLOAT,price_INR))/1000000,'N2')+'INR_MILLIONS'
       AS TOTAL_REVENUE
       FROM fact_swiggy_orders

       ---AVERAGE DISH PRICE--

       SELECT 
       FORMAT(AVG(CONVERT(FLOAT,price_inr)),'N2')+'INR'
       AS AVERAGE_DISH_PRICE
       FROM fact_swiggy_orders

      ---AVERAGE RATING---

      SELECT AVG(rating) AS AVG_RATING
      FROM fact_swiggy_orders

      ---DEEP DIVE ANALYSIS ON BUISINESS IDEAS---

      ---MONTHLY ORDER TRENDS---

      SELECT
      d.year,
      d.month,
      d.month_name,
      count(*) AS Total_Monthly_orders FROM
      fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY d.year,
      d.month,
      d.month_name
      ORDER BY count(*) DESC

      ---MONTHLY REVENUE--

      SELECT 
      d.year,
      d.month,
      d.month_name,
      FORMAT(SUM(CONVERT(FLOAT,price_inr))/1000000,'N2')+'INR_MILLIONS' AS Monthly_Revenue FROM
      fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY d.year,
      d.month,
      d.month_name 
      ORDER BY Monthly_Revenue DESC

      ---QUARTERLY TRENDS--

      SELECT
      d.year,
      d.quarter,
      count(*) AS QUARTERLY_ORDERS FROM
      fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY
      d.year,
      d.quarter

      ---QUARTERLY REVENUE---

      SELECT 
      d.year,
      d.quarter,
      FORMAT(SUM(CONVERT(FLOAT,price_inr))/1000000,'N2')+'INR_MILLION' AS Total_Quarterly_Revenue
      from fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY 
      d.year,
      d.quarter
      ORDER BY 
      Total_Quarterly_Revenue DESC

      ---YEARLY TREND--

      SELECT 
      d.year,
      count(*) AS Yearly_Orders
      FROM fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY
      d.year

      select * from dim_date

      ---ORDERS BY DAY OF WEEK---

      SELECT
      DATENAME(WEEKDAY,d.full_date) AS Weekday,
      count(*) AS Weekday_orders
      FROM fact_swiggy_orders f
      JOIN dim_date d ON d.date_id=f.date_id
      GROUP BY DATENAME(WEEKDAY, d.full_date),DATEPART(WEEKDAY, d.full_date)
      ORDER BY DATEPART(WEEKDAY, d.full_date)

      ---LOCATION BASED ANALYSIS--
      ---TOP 10 CITIES---

      SELECT TOP 10
      l.city,
      count(*) AS Total_Orders
      FROM fact_swiggy_orders f
      JOIN dim_location l ON l.location_id=f.location_id
      GROUP BY l.city
      ORDER BY Total_Orders DESC

      ---TOP 10 STATES---

      SELECT TOP 10
      l.state,
      count(*) AS Total_Orders
      FROM fact_swiggy_orders f
      JOIN dim_location l ON l.location_id=f.location_id
      GROUP BY l.state
      ORDER BY Total_Orders DESC

      ---REVENUE CONTRIBUTION BY STATE---

      SELECT 
      l.state,
      FORMAT(SUM(CONVERT(FLOAT,price_inr))/1000000,'N2')+'INR_MILLION' AS Revenue
      FROM fact_swiggy_orders f
      JOIN dim_location l ON l.location_id=f.location_id
      GROUP BY l.state
      ORDER BY Revenue DESC

      ---RESTAURANT ANALYSIS---
      ---TOP 10 RESTAURANT---

      SELECT TOP 10
      r.restaurant_name,
      FORMAT(SUM(CONVERT(FLOAT,f.price_inr))/1000000,'N2')+'INR_MILLION' AS Total_Revenue
      FROM  fact_swiggy_orders f
      JOIN dim_restaurant r ON r.restaurant_id=f.restaurant_id
      GROUP BY r.restaurant_name
      ORDER BY Total_Revenue DESC

      ---TOP CATEGORIES BY ORDER VOLUME---

      SELECT
      c.category_name,
      count(*) AS Total_Orders
      FROM fact_swiggy_orders f
      JOIN dim_category c ON c.category_id=f.category_id
      GROUP BY c.category_name
      ORDER BY Total_Orders DESC

      ---MOST ORDER DISH---

      SELECT 
      d.dish_name,
      COUNT(*) AS Total_Orders
      FROM fact_swiggy_orders f
      JOIN dim_dish_name d ON d.dish_name_id=f.dish_name_id
      GROUP BY d.dish_name
      ORDER BY Total_Orders DESC

      USE Swiggy_data

      ---CUISINE PERFORMANCE---
 
    SELECT 
    c.category_name,
    COUNT(*) AS Total_Orders,
    AVG(f.rating) AS Average_rating
    FROM fact_swiggy_orders f
    JOIN dim_category c ON c.category_id=f.category_id
    GROUP BY c.category_name
    ORDER BY Total_Orders DESC

    select * from dim_category

    ---TOTAL ORDER BY PRICE RANGE---
    SELECT
    CASE 
        WHEN CONVERT(FLOAT,price_inr) < 100 THEN 'Under 100'
        WHEN CONVERT(FLOAT,price_inr) BETWEEN 100 AND 199 THEN '100 - 199'
        WHEN CONVERT(FLOAT,price_inr) BETWEEN 200 AND 299 THEN '200 - 299'
        WHEN CONVERT(FLOAT,price_inr) BETWEEN 300 AND 399 THEN '300 - 399'
        WHEN CONVERT(FLOAT,price_inr) BETWEEN 400 AND 499 THEN '400 - 499'
        ELSE '500+'
        END AS price_range,
        COUNT(*) AS Total_Orders
        FROM fact_swiggy_orders 
    GROUP BY 
    CASE 
       WHEN CONVERT(FLOAT,price_inr) < 100 THEN 'Under 100'
       WHEN CONVERT(FLOAT,price_inr) BETWEEN 100 AND 199 THEN '100 - 199'
       WHEN CONVERT(FLOAT,price_inr) BETWEEN 200 AND 299 THEN '200 - 299'
       WHEN CONVERT(FLOAT,price_inr) BETWEEN 300 AND 399 THEN '300 - 399'
       WHEN CONVERT(FLOAT,price_inr) BETWEEN 400 AND 499 THEN '400 - 499'
       ELSE '500+'
       END
       ORDER BY Total_Orders

---RATING COUNT---

SELECT
rating,
COUNt(*) AS rating_count
FROM fact_swiggy_orders
GROUP BY rating
ORDER BY rating_count DESC




      



