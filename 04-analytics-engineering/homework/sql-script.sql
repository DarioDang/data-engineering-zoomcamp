-- Question 03: count of records in the fct_monthly_zone_revenue model -- 
select count(*) 
from `de-zoomcamp-project-485521.dbt_prod.fact_monthly_zone_revenue`

--Question 04: pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020 --
select
    pickup_zone, sum(revenue_monthly_total_amount) as total_revenue_2020
from `de-zoomcamp-project-485521.dbt_prod.fact_monthly_zone_revenue` 
where service_type = 'Green'
  and extract( year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 DESC
limit 1;

-- Question 05: what is the total number of trips (total_monthly_trips) for Green taxis in October 2019? --
select sum(total_monthly_trips) AS total_count
from `de-zoomcamp-project-485521.dbt_prod.fact_monthly_zone_revenue` 
where service_type = 'Green' and extract(year from revenue_month) = 2019 
and extract(month from revenue_month) = 10; 

-- Question 06: count of records in stg_fhv_tripdata -- 
select count(*)
from `de-zoomcamp-project-485521.dbt_prod.fact_monthly_zone_revenue` 
