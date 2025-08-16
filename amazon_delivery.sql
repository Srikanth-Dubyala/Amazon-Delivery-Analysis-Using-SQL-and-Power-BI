select * from amazon ;

with duplicates_cte as
(
select * ,
row_number() over (partition by MyUnknownColumn, Order_ID, Agent_Age, Agent_Rating, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude, Order_Date, Order_Time, Pickup_Time, Weather, Traffic, Vehicle, Area, Delivery_Time, Category) as row_num
from amazon
)
select * from duplicates_cte 
where row_num > 1;

CREATE TABLE `amazon_1` (
  `MyUnknownColumn` int DEFAULT NULL,
  `Order_ID` text,
  `Agent_Age` int DEFAULT NULL,
  `Agent_Rating` double DEFAULT NULL,
  `Store_Latitude` double DEFAULT NULL,
  `Store_Longitude` double DEFAULT NULL,
  `Drop_Latitude` double DEFAULT NULL,
  `Drop_Longitude` double DEFAULT NULL,
  `Order_Date` text,
  `Order_Time` text,
  `Pickup_Time` text,
  `Weather` text,
  `Traffic` text,
  `Vehicle` text,
  `Area` text,
  `Delivery_Time` int DEFAULT NULL,
  `Category` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from amazon_1 ;

insert into  amazon_1
select * 
from amazon;

select * from amazon_1;

alter table amazon_1
drop column  MyUnknownColumn;

select * from amazon_1;

select * from amazon_1
where Weather is Null ;


-- the total number of orders in the dataset

select count(Order_ID) as number_of_orders
from amazon_1;

--  What is the average delivery time for all orders?

select round(avg(Delivery_Time)) as avg_delivery_time
from amazon_1 ;

-- Find all orders where the Vehicle is a 'scooter'.

select * from amazon_1 ;

select count(Order_ID) as no_of_orders_delivery_by_scooter
from amazon_1
where Vehicle = 'scooter '  ;

-- • List all orders with a Delivery_Time of over 150 minutes
 
select count(Order_ID) as no_of_order_delivery_time_morethan_150
from amazon_1
where Delivery_Time >150;

-- Find all orders placed on '2022-03-19'.

select * from amazon_1 ;

select count(Order_ID) as no_of_orders_on_that_date
from amazon_1
where Order_Date = '19-03-2022';

--  What is the highest Agent_Rating in the dataset

select Max(Agent_Rating) as highest_rating
from amazon_1;

-- • How many orders were delivered under 'Sunny' weather conditions

select count(Order_ID) as no_of_orders_on_sunny_day
from amazon_1
where Weather ='Sunny';

-- • Find all orders with a Traffic status of 'High' and an Agent_Rating of 4.5

select count(Order_ID) as no_of_orders_on_traffic_high_with_agenet_rating_4_point_5
from amazon_1
where Agent_Rating =4.5  and Traffic = 'High ';


-- Which Area has the most orders?

select Area ,count(Order_ID) as area_with_most_no_of_orders
from amazon_1
group by Area
order by  area_with_most_no_of_orders desc
limit 1 ;


--  • Calculate the total number of orders for each Category

select * from amazon_1;

select Category ,count(Order_ID) as no_oforders_per_category 
from amazon_1
group by Category
order by  no_oforders_per_category  desc ;

--  What is the average Agent_Rating for each Weather condition

select Weather ,round(avg(Agent_Rating),2) as avg_rating
from amazon_1 
group by  Weather
order by avg_rating desc ;

-- Find the average Delivery_Time for each Vehicle type

select Vehicle,round(avg(Delivery_Time),2) as avg_delivery_time
from amazon_1
group by Vehicle
order by avg_delivery_time ;

-- Group the orders by Area and find the total number of orders in each

select Area ,count(Order_ID) as area_with_most_no_of_orders
from amazon_1
group by Area
order by  area_with_most_no_of_orders desc;

-- What is the maximum Delivery_Time for each Traffic condition?
select * from amazon_1 ;

select Traffic,Max(Delivery_Time) as max_delivery_time
from amazon_1
group by Traffic ;

--  Find the top 5 Agents with the highest average Agent_Rating.
select Agent_Age ,round(avg(Agent_Rating),4) as avg_rating
from amazon_1
group by Agent_Age
order by avg_rating desc
limit 5  ;

--  • What is the average Agent_Age for agents delivering 'Clothing'
select round(avg(Agent_Age)) as avg_age_of_person_delivering_clothing
from amazon_1
where Category  = 'Clothing' ;

--  Count the number of orders for each Order_Date.
select Order_Date ,count(Order_ID) as no_of_orders
from amazon_1
group by Order_Date 
order by no_of_orders desc;

-- no of orders by month
select monthname(str_to_date(Order_Date ,'%d-%m-%Y')) as month ,count(Order_ID) as no_of_orders
from amazon_1
group by  monthname(str_to_date(Order_Date ,'%d-%m-%Y'))
order by no_of_orders  desc ;

-- • Find the total number of orders for each Traffic condition, but only for Urban areas

select Traffic ,count(Order_ID) as no_of_orders_in_urban
from amazon_1
where Area = 'Urban '
group  by  Traffic 
order by  no_of_orders_in_urban ;

-- • What is the average Agent_Rating for agents whose age is above 35
select * from amazon_1;

select round(avg(Agent_Rating),4) as avg_rating_ofagents_above_35
from amazon_1
where Agent_Age >35 ;

-- What is the average Agent_Rating for agents whose age is below and eqaul to  35
select round(avg(Agent_Rating),4) as avg_rating_ofagents_above_35
from amazon_1
where Agent_Age <=35 ;

--  Write a query to find the Category that has the longest average Delivery_Time
select * from amazon_1;

with category_cte as
(
select Category ,round(avg(Delivery_Time)) as average_delivery_time ,
rank() over (order by avg(Delivery_Time)  desc ) as rnk
from amazon_1
group by Category
)
select Category  , average_delivery_time from category_cte
where  rnk =1;
 
 --  find all orders where the Delivery_Time is above the overall average  Delivery_Time?

select  count(Order_ID) as no_of_orders_above_average_delivery_time
from amazon_1
where Delivery_Time > (select round(avg(Delivery_Time)) as avg_delivery_time from amazon_1);

-- Find the average Agent_Rating for each Area, but only include Areas with more than 1000 orders

select Area , avg_rating_morethan_1000_orders  
from (
select Area ,round(avg(Agent_Rating),4) as avg_rating_morethan_1000_orders ,count(Order_ID) as no_of_orders
from amazon_1
group by Area
having count(Order_ID) > 1000
order by no_of_orders desc
) as avg_rate ;

--  • What is the percentage of total orders for each Category?
with total_cte as
(
select count(*) as total_orders
from amazon_1
),
total_by_category as
(
select Category ,count(Order_ID) as no_of_orders
from amazon_1
group by Category 
)
select c.Category , (c.no_of_orders  * 100 )/t.total_orders as percentage_of_orders
from total_cte  t
cross join total_by_category c 
order by  percentage_of_orders desc ;

-- Find the total number of orders and average Delivery_Time for each Order_Date in a single query

select * from amazon_1 ;

select Order_Date ,count(Order_ID) as total_no_of_orders ,avg(Delivery_Time) as avg_delivery_time
from amazon_1
group by Order_Date
order by month(str_to_date(Order_Date ,'%d-%m-%Y')) ,total_no_of_orders desc;


--  Write a query to classify Delivery_Time into 'Fast' (under 120 minutes) or 'Normal' (120 minutes ormore).

select count(Order_ID) as no_of_orders,
case
when  Delivery_Time < 120 then 'Fast Delivery'
else 'Normal'
end as delivery_classification
from amazon_1
group by delivery_classification ;

--  Find the top 3 Areas based on the number of motorcycle deliveries.

select * from amazon_1;

with motor_cycle_cte as
(
select Area ,Vehicle ,count(Order_ID) as no_of_orders ,
rank() over (partition by Area order  by count(Order_ID) desc ) as rnk
from amazon_1 
group by  Area ,Vehicle
)
select Area ,Vehicle  from motor_cycle_cte 
where rnk = 1
limit 3 ;

--  • What is the average Delivery_Time for orders delivered on weekends (Saturday and Sunday)

select * from amazon_1 ;

select dayname(str_to_date(Order_Date ,'%d-%m-%Y')) as days ,avg(Delivery_Time) as avg_delivery_time
from amazon_1
where dayname(str_to_date(Order_Date ,'%d-%m-%Y')) in ('Saturday' ,'Sunday')
group by dayname(str_to_date(Order_Date ,'%d-%m-%Y'))  ;


-- Write a query to find the Order_IDs of the orders with the top 10 longest Delivery_Times

with delivery_cte as
(
select Order_ID ,Max(Delivery_Time) as longest_delivery_time,
rank() over(order by  Max(Delivery_Time)  desc) as rnk
from amazon_1
group by Order_ID 
)
select Order_ID , longest_delivery_time from delivery_cte 
where rnk =1 ;

--  calculate the number of orders and average Agent_Rating for each Agent_Age group 

select count(Order_ID) as no_of_orders ,round(avg(Agent_Rating),4),
case
when Agent_Age between 18 and 35 then 'Young'
when Agent_Age between 36 and 50 then 'Middle-Aged'
else 'Old'
end as age_distribution
from amazon_1
group by age_distribution  ;

-- • Find all orders where the Pickup_Time is exactly 15 minutes after the Order_Time 

select count(Order_ID) as no_of_orders
from amazon_1
where timediff(str_to_date(Pickup_Time,'%H:%i:%s') ,str_to_date(Order_Time ,'%H:%i:%s')) = '00:15:00';

--  Write a query to rank agents by their average rating.

with ranking_cte as
(
select Agent_Age ,round(avg(Agent_Rating),5) as avg_rating,
dense_rank() over ( order by avg(Agent_Rating) desc ) as rnk
from amazon_1
group by Agent_Age
)
select Agent_Age ,avg_rating ,rnk
from ranking_cte ;

--  Find the number of orders per month and order them by the month with the most orders.

select monthname(str_to_date(Order_Date,'%d-%m-%Y')) as month ,count(Order_Id) as no_of_orders
from amazon_1
group by monthname(str_to_date(Order_Date,'%d-%m-%Y'))
order by no_of_orders desc ;

--  How would you identify the Agent_IDs who have delivered at least one order for every Category
select Distinct Category from amazon_1;

select Distinct Agent_Age 
from amazon_1
group by Agent_Age 
having count(Distinct Category ) = (select count(Distinct Category) from amazon_1);


--  • Which hour of the day has the highest number of orders
select time(str_to_date(Order_Time ,'%H:%i:%s')) as time_of_day ,count(Order_ID) as no_of_orders
from amazon_1
group by time(str_to_date(Order_Time ,'%H:%i:%s')) 
order by no_of_orders desc ;


with time_distribution_cte as
(
select count(Order_ID) as no_of_orders ,
case 
when time(str_to_date(Order_Time ,'%H:%i:%s')) between '04:00:00' and '12:00:00' then 'Morning'
when time(str_to_date(Order_Time ,'%H:%i:%s')) between '12:00:00' and '19:00:00' then 'Evening'
when time(str_to_date(Order_Time ,'%H:%i:%s')) between '19:00:00' and '22:00:00' then 'Night'
else 'Mid-Night'
end as day_distribution
from amazon_1
group by  day_distribution
order by no_of_orders desc
)
select  * from   time_distribution_cte ;

--  Calculate the time gap between consecutive orders for each agent

select * from amazon_1 ;

select Agent_Age ,Order_ID,Order_Time,
timestampdiff(minute ,lag(str_to_date(Order_Time ,'%H:%i:%s')) over (partition by Agent_Age order by str_to_date(Order_Time,'%H:%i:%s') ),str_to_date(Order_Time,'%H:%i:%s')) as time_gap
from amazon_1 ;

--  • Find the number of orders delivered within 30 minutes.

select count(Order_ID) as no_of_orders
from amazon_1
where Delivery_Time <= 30 ;

 -- Which agents have the highest number of delayed deliveries (over 2 hours)?
 
 select Agent_Age 
 from amazon_1
 where Delivery_Time > 120 ;
 
 --  • Find the most common order categories per Area.
 with common_order
as
(
select Category ,Area ,count(Order_ID) as no_of_Orders,
rank() over (partition by Area order by count(Order_ID) desc ) as rnk
from amazon_1
group by Category ,Area
)
select * from common_order
where rnk = 1 ;

--  Identify the average delivery time per Category and Weather condition
select Category ,Weather ,avg(Delivery_Time) as avg_delivery_time
from amazon_1
group by Category ,Weather 
order  by  avg_delivery_time desc  ;



















