--querys

--- 1- What is the country with the highest number of Covid-19 cases per 100 000 Habitants at
--  31/07/2020?
 -- answer = Panama
  
select * from covidcountry c 
 where year_week='2020-'||(SELECT date_part('week',date '2020-07-31'))
 and indicatortypes = 'cases'
 order by rate_14_day desc limit 1


-- 2- What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
 --- answer = Jordan, Myanmar, Taiwan, Thailand, China, Niger, Mongolia, Vietnam, Uganda, New Zealand 
 select * from covidcountry c 
 where year_week='2020-'||(SELECT date_part('week',date '2020-07-31'))
 and indicatortypes = 'cases'
 and weekly_count >0 -- lowest case , ie , great than zero
  order by rate_14_day asc limit 10

--3- What is the top 10 countries with the highest number of cases among the top 20 richest
--countries (by GDP per capita)? answer = United Kingdom, France, Germany, Netherlands, Canada, Japan, Belgium, Switzerland, Austria, Ireland
select * 
  from (select country gdb, cumulative_count from countryworld_lastest_cases
	  order by gdp desc limit 20) as d
order by cumulative_count desc limit 10


--4- List all the regions with the number of cases per million of inhabitants and display information
-- on population density, for 31/07/2020.

select region, sum(cumulative_count) cases, sum(density)/count(*) density from covidcountry cc , countryworld cw 
 where year_week='2020-'||(SELECT date_part('week',date '2020-07-31'))
 and indicatortypes = 'cases'
 and upper(cw.country) = upper(cc.country)
 group by region


answer : 
region || cases , density
ASIA (EX. NEAR EAST)	3105529,	570.5809523809523810
BALTICS	4263,	39.8333333333333333
C.W. OF IND. STATES	1258282,	60.9272727272727273
EASTERN EUROPE	167329,	101.3444444444444444
LATIN AMER. & CARIB	5016234,	130.9500000000000000
NEAR EAST	1124771,	174.6142857142857143
NORTHERN AFRICA	157503,	38.9333333333333333
NORTHERN AMERICA	117040,	414.7666666666666667
OCEANIA	19729,	67.6000000000000000
SUB-SAHARAN AFRICA	754043,	84.7973684210526316
WESTERN EUROPE	1658833,	952.0428571428571429

 --5- Query the data to find duplicated records.
 select year_week, country , count(*)
 from covidcountry
 group by year_week, country
 having count(*)>1


--6- Analyze the performance of all the queries and describes what you see. Get improvements
--suggestions.
answer :  all tables is little just create a index for compare two tables CREATE INDEX covidcountry_country_idx ON public.covidcountry (country); 



