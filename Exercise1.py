import json
from os import path, getenv
from dotenv import load_dotenv
import requests
import psycopg2 as pg
import pandas as pd

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))
PGHOST = getenv('PGHOST')
PGDATABASE = getenv('PGDATABASE')
PGPASSWORD = getenv('PGPASSWORD')
PGUSER = getenv('PGUSER')
def connect_db():
    try:
        conn_string = "host="+ PGHOST +" port="+ "5432" +" dbname="+ PGDATABASE +" user=" + PGUSER \
                    +" password="+ PGPASSWORD
        conn=pg.connect(conn_string) 
        return conn
    except Exception as e:
        print(e)

#  country varchar(150),
#  country_code varchar(3),
#  continent varchar(30),
#  population bigint,
#  indicatortypes varchar(20),
#  weekly_count int,
#  year_week varchar(7),
#  cumulative_count bigint,
#  rate_14_day numeric (11,8) ,
#  source_covid varchar(100)        
def  postgres_insert_covidcountry(conn,country,country_code, continent, population,indicatortypes, weekly_count,year_week, cumulative_count,rate_14_day, source_covid  ):
    """ INSERT INTO covidcountry"""
    try:      
        cursor = conn.cursor()
        _query =  """ INSERT INTO covidcountry (country,country_code, continent, population,indicatortypes, weekly_count,year_week, cumulative_count,rate_14_day, source_covid  )  """ \
                  " values (%s,%s, %s, %s, %s,%s,%s, %s, %s, %s)"        
        record_to_insert = (country,country_code, continent, population,indicatortypes, weekly_count,year_week, cumulative_count,rate_14_day, source_covid )
        cursor.execute(_query, record_to_insert)
        conn.commit();
    except Exception as e :
        print(e) ;   
def get_source1():
    r =requests.get('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/')
    if r.status_code == 200:
        r_dictionary= r.json()
        conn = connect_db()
        for item in r_dictionary:        
            country=item['country']
            if 'country_code' in item.keys():
                country_code=item['country_code']
            continent=item['continent']
            population=item['population']
            indicatortypes=item['indicator']
            weekly_count=item['weekly_count']
            year_week=item['year_week']
            cumulative_count=item['cumulative_count']
            if 'rate_14_day' in item.keys():
                rate_str =  item['rate_14_day']
                rate_float=float(rate_str)
                rate_14_day = round(rate_float,5)
            else:
                rate_14_day = '0'
            source_covid=item['source']
            postgres_insert_covidcountry(conn,country,country_code, continent, population,indicatortypes, weekly_count,year_week, cumulative_count,rate_14_day, source_covid )


def get_datasource2():
    """ https://www.kaggle.com/fernandol/countries-of-the-world/data?select=countries+of+the+world.csv       """
    #url = 'https://www.kaggle.com/fernandol/countries-of-the-world/data?select=countries+of+the+world.csv'
    df = pd.read_csv('countries of the world.csv', delimiter=',')
    df.fillna(0,inplace=True)
    df.columns = (["country","region","population","area","density","coastline","migration","infant_mortality","gdp","literacy","phones","arable","crops","other","climate","birthrate","deathrate","agriculture","industry","service"])
    df.country = df.country.str.strip()
    df.region = df.region.str.strip()
    df.population = df.population.astype(int)
    df.area = df.area.astype(int)
    df.density = df.density.str.replace(",",".").astype(float)
    df.coastline = df.coastline.str.replace(",",".").astype(float)
    df.migration = df.migration.str.replace(",",".").astype(float)
    df.infant_mortality = df.infant_mortality.str.replace(",",".").astype(float)
    df.literacy = df.literacy.str.replace(",",".").astype(float)
    df.phones = df.phones.str.replace(",",".").astype(float)
    df.arable = df.arable.str.replace(",",".").astype(float)
    df.crops = df.crops.str.replace(",",".").astype(float)
    df.other = df.other.str.replace(",",".").astype(float)
    df.climate = df.climate.str.replace(",",".").astype(float)
    df.birthrate = df.birthrate.str.replace(",",".").astype(float)
    df.deathrate = df.deathrate.str.replace(",",".").astype(float)
    df.agriculture = df.agriculture.str.replace(",",".").astype(float)
    df.industry = df.industry.str.replace(",",".").astype(float)
    df.service = df.service.str.replace(",",".").astype(float)
    print(len(df))
    conn = connect_db()
    for i in range(len(df)-1):
        country = df.iloc[i,0]
        region = df.iloc[i,1]
        population = df.iloc[i,2]
        areacountry = df.iloc[i,3]
        density = df.iloc[i,4]
        coastline = df.iloc[i,5]
        migration = df.iloc[i,6]
        infant_mortality = df.iloc[i,7]
        gdp = df.iloc[i,8]
        literacy = df.iloc[i,9]
        phones = df.iloc[i,10]
        arable = df.iloc[i,11]
        crops = df.iloc[i,12]
        other = df.iloc[i,13]
        climate = df.iloc[i,14]
        birthrate = df.iloc[i,15]
        deathrate = df.iloc[i,16]
        agriculture = df.iloc[i,17]
        industry = df.iloc[i,18]
        service = df.iloc[i,19]      
        #print(country,region,population,areacountry,density,coastline,migration,infant_mortality,gdp,literacy,phones,arable,crops,other,climate,birthrate,deathrate,agriculture,industry,service)  
        postgres_insert_coutryworld(conn, country,region,population,areacountry,density,coastline,migration,infant_mortality,gdp,literacy,phones,arable,crops,other,climate,birthrate,deathrate,agriculture,industry,service)
        # for j in range(len(df.columns)):            
        #     print(df.iloc[i,j])
        #break
  
       

def  postgres_insert_coutryworld(conn, country,region,population,areacountry,density,coastline,migration,infant_mortality,gdp,literacy,phones,arable,crops,other,climate,birthrate,deathrate,agriculture,industry,service):
    """ INSERT INTO countryworld"""
    try:      
        cursor = conn.cursor()
        _query =   """  INSERT INTO countryworld ( country, region,population, areacountry, density,coastline,migration,infant_mortality,gdp,literacy,phones,arable,crops,other,climate, birthrate,deathrate,agriculture,industry,service )  """ \
                  " values (%s , %s , %s, %s ,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) "
        record_to_insert = ( country, region, int(population), int(areacountry),  density,coastline,migration,infant_mortality,gdp,literacy,phones,arable,crops,other,climate, birthrate,deathrate,agriculture,industry,service)
        print(record_to_insert)
        cursor.execute(_query, record_to_insert)
        conn.commit()
        cursor.close;
    except Exception as e :        
        print('banco',e) 
        
    

if __name__ == "__main__":
    get_source1()
    get_datasource2()
