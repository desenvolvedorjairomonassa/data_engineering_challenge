		
CREATE INDEX covidcountry_country_idx ON public.covidcountry (country);

create view countryworld_lastest_cases as 
select cw.*, cc.cumulative_count, cc.rate_14_day, cc.year_week  from countryworld cw ,
	(select * from covidcountry cc
		where year_week =  ( 
				select max(year_week) yearweek from covidcountry latest_covid
				where indicatortypes = 'cases'
				and latest_covid.country  = cc.country
				group by country )
		and indicatortypes = 'cases') as cc 
where upper(cw.country) = upper(cc.country);

select * from countryworld_lastest_cases ;