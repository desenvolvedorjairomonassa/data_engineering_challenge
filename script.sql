create table covidcountry (
 id bigserial,
 country varchar(150),
 country_code varchar(3),
 continent varchar(30),
 population bigint,
 indicatortypes varchar(20),
 weekly_count int,
 year_week varchar(7),
 cumulative_count bigint,
 rate_14_day numeric (15,8) ,
 source_covid varchar(120)

)
;

create table countryworld (
 id bigserial,
 country varchar(150),
 region varchar(100),
 population bigint,
 areacountry numeric (15,4) ,
 density numeric (15,4) ,
 coastline numeric (15,4) ,
 migration numeric (15,4) ,
 infant_mortality numeric (15,4) ,
 gdp  numeric(15,4),
 literacy numeric(15,4),
 phones numeric(10,4),
 arable numeric (10,4), 
 crops numeric (10,4),
 other numeric (10,4),
 climate numeric(10,4) ,
 birthrate	numeric (10,4),
 deathrate	numeric (10,4),
 agriculture numeric (10,4),
 industry	numeric (10,4),
 service numeric (10,4)
 );
 
 