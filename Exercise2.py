import json
from os import path, getenv
from dotenv import load_dotenv
import requests
import psycopg2 as pg
import pandas as pd
import Exercise1_copy as e

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

def max_year_weekly (conn):
    _query = """ select max(year_week) from covidcountry """
    cursor = conn.cursor()
    cursor.execute(_query)
    point = cursor.fetchone()[0]
    return point

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
        print(e) 
        
def record_dict(dict,conn):
    for item in dict:        
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
        
def get_source1():
    r =requests.get('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/')
    if r.status_code == 200:
        r_dictionary= r.json()            
    return r_dictionary    
    #r_dic_filter = dict(filter(lambda val: val[0] % 3 == 0, my_dict.items()))
    #print(r_dictionary)    
        
if __name__ == "__main__":
    conn = connect_db()
    max= max_year_weekly(conn)
    dic=get_source1()   
    #filter elements year-week  great than max year-week database
    filtered = filter(lambda item: item['year_week'] > '2021-43', dic)    
    e.record_dict(list(filtered),conn)
    