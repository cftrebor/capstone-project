import configparser
import logging
import psycopg2
import time 
from sql_queries import *


def load_vaccine_dim(cur, sql):
    cur.execute(sql) 


def load_cases_deaths_dim(cur, sql):
    cur.execute(sql)


def load_bing_dim(cur, sql):
    cur.execute(sql)


def load_bing_dim_queries(cur):
    # get list of Bing queries per date and location and store as comma separated string
    # and save to query column of bing_dim table
    cur.execute('SELECT DISTINCT date, state_abbr FROM bing_dim;')
    results = cur.fetchall()
    total_results = len(results)
    logging.info(f'total distinct dates, state_abbr from bing_dim is: {total_results}')

    # iterate through each date, state_abbr pair
    for result in results:
        # set date and state values to variable for use
        date_val = str(result[0])
        state_val = str(result[1])

        # run SELECT on Bing stage table to grab list of queries for current date/state_abbr pair
        cur.execute("""SELECT query FROM bing_search_stage WHERE date = %s AND state_abbr = %s ORDER BY popularityscore desc limit 5;""", (date_val, state_val))
        queries = cur.fetchall()
        
        # initialize variables for following loop
        total_queries = len(queries)
        query_string = ""
        # iterate through each query returned for current date/state_abbr to build comma separated list of queries
        for query in queries:
            query_string += str(query[0])
            total_queries -= 1
            if total_queries > 0:
                query_string += ", "
        
        cur.execute("""UPDATE bing_dim SET query = %s WHERE date = %s AND state_abbr = %s;""", (query_string, date_val, state_val))
        logging.info(f'{date_val} {state_val} {query_string} has been processed.')

def load_covid_fact(cur, sql):
    cur.execute(sql)
    
def copy_s3_vacc_stage(cur, role, bucket, dataset):
    sql = f"""
    copy vacc_stage (
         Date 
        ,MMWR_week
        ,Location
        ,Distributed 
        ,Distributed_Janssen 
        ,Distributed_Moderna 
        ,Distributed_Pfizer 
        ,Distributed_Unk_Manuf 
        ,Dist_Per_100K 
        ,Distributed_Per_100k_12Plus 
        ,Distributed_Per_100k_18Plus 
        ,Distributed_Per_100k_65Plus 
        ,Administered 
        ,Administered_12Plus 
        ,Administered_18Plus 
        ,Administered_65Plus 
        ,Administered_Janssen 
        ,Administered_Moderna 
        ,Administered_Pfizer 
        ,Administered_Unk_Manuf 
        ,Admin_Per_100K 
        ,Admin_Per_100k_12Plus 
        ,Admin_Per_100k_18Plus 
        ,Admin_Per_100k_65Plus 
        ,Recip_Administered 
        ,Administered_Dose1_Recip 
        ,Administered_Dose1_Pop_Pct 
        ,Administered_Dose1_Recip_12Plus 
        ,Administered_Dose1_Recip_12PlusPop_Pct 
        ,Administered_Dose1_Recip_18Plus 
        ,Administered_Dose1_Recip_18PlusPop_Pct 
        ,Administered_Dose1_Recip_65Plus 
        ,Administered_Dose1_Recip_65PlusPop_Pct 
        ,Series_Complete_Yes 
        ,Series_Complete_Pop_Pct 
        ,Series_Complete_12Plus 
        ,Series_Complete_12PlusPop_Pct 
        ,Series_Complete_18Plus 
        ,Series_Complete_18PlusPop_Pct 
        ,Series_Complete_65Plus 
        ,Series_Complete_65PlusPop_Pct 
        ,Series_Complete_Janssen 
        ,Series_Complete_Moderna 
        ,Series_Complete_Pfizer 
        ,Series_Complete_Unk_Manuf 
        ,Series_Complete_Janssen_12Plus 
        ,Series_Complete_Moderna_12Plus 
        ,Series_Complete_Pfizer_12Plus 
        ,Series_Complete_Unk_Manuf_12Plus 
        ,Series_Complete_Janssen_18Plus 
        ,Series_Complete_Moderna_18Plus 
        ,Series_Complete_Pfizer_18Plus 
        ,Series_Complete_Unk_Manuf_18Plus 
        ,Series_Complete_Janssen_65Plus 
        ,Series_Complete_Moderna_65Plus 
        ,Series_Complete_Pfizer_65Plus 
        ,Series_Complete_Unk_Manuf_65Plus 
        ,Additional_Doses 
        ,Additional_Doses_Vax_Pct 
        ,Additional_Doses_18Plus  
        ,Additional_Doses_18Plus_Vax_Pct 
        ,Additional_Doses_50Plus 
        ,Additional_Doses_50Plus_Vax_Pct 
        ,Additional_Doses_65Plus 
        ,Additional_Doses_65Plus_Vax_Pct 
        ,Additional_Doses_Moderna 
        ,Additional_Doses_Pfizer 
        ,Additional_Doses_Janssen 
        ,Additional_Doses_Unk_Manuf 
        ,Administered_Dose1_Recip_5Plus 
        ,Administered_Dose1_Recip_5PlusPop_Pct 
        ,Series_Complete_5Plus 
        ,Series_Complete_5PlusPop_Pct 
        ,Administered_5Plus 
        ,Admin_Per_100k_5Plus 
        ,Distributed_Per_100k_5Plus 
        ,Series_Complete_Moderna_5Plus 
        ,Series_Complete_Pfizer_5Plus 
        ,Series_Complete_Janssen_5Plus 
        ,Series_Complete_Unk_Manuf_5Plus 
    )
    from 's3://{bucket}/{dataset}'
    iam_role '{role}'
    delimiter '|'
    DATEFORMAT AS 'MM/DD/YYYY'
    ;
    """
    cur.execute(sql)

def copy_s3_cases_deaths_stage(cur, role, bucket, dataset):
    sql = f"""
    copy cases_deaths_by_state_stage (
         submission_date 
        ,state
        ,tot_cases 
        ,conf_cases 
        ,prob_cases 
        ,new_case 
        ,pnew_case 
        ,tot_death 
        ,conf_death 
        ,prob_death 
        ,new_death 
        ,pnew_death 
        ,created_at 
        ,consent_cases 
        ,consent_deaths
    )
    from 's3://{bucket}/{dataset}'
    iam_role '{role}'
    delimiter '|'
    DATEFORMAT AS 'MM/DD/YYYY'
    TIMEFORMAT AS 'MM/DD/YYYY HH:MI:SS'
    ;
    """
    cur.execute(sql)

def copy_s3_bing_search_stage(cur, role, bucket, dataset):
    sql = f"""
    copy bing_search_stage (
         date 
        ,query 
        ,State
        ,Country
        ,PopularityScore
        ,State_Abbr
    )
    from 's3://{bucket}/{dataset}'
    iam_role '{role}'
    delimiter '|'
    DATEFORMAT AS 'YYYY-MM-DD'
    ;
    """
    cur.execute(sql)


def main():
    """
    Load dimension and fact tables from staging tables.
    """

    # configure log file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(filename=f'./logs/etl-{timestr}.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(levelname)-4s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # connect to database using config
    logging.info('Connecting to database.')
    config = configparser.ConfigParser()
    config.read('config')
    iam_role = config['AWS']['iam_role']
    s3_bucket = config['AWS']['s3_bucket']
    
    # s3 datasets
    vaccine_ds = "vacc_df.csv"
    cases_deaths_ds = "cases_deaths_df.csv"
    bing_search_ds = "bing_search_df.csv"    
    
    # connect to Redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DEV'].values()))
    cur = conn.cursor()
    
    logging.info('Copying s3 data to vaccine staging table.')
    copy_s3_vacc_stage(cur, iam_role, s3_bucket, vaccine_ds)
    conn.commit()
    
    logging.info('Copying s3 data to cases and deaths staging table.')
    copy_s3_cases_deaths_stage(cur, iam_role, s3_bucket, cases_deaths_ds)
    conn.commit()
    
    logging.info('Copying s3 data to Bing search staging table.')
    copy_s3_bing_search_stage(cur, iam_role, s3_bucket, bing_search_ds)
    conn.commit()

    logging.info('Loading vaccine_dim table.')
    load_vaccine_dim(cur, vaccine_dim_insert)
    conn.commit()

    logging.info('Loading cases_deaths_dim table.')
    load_cases_deaths_dim(cur, cases_deaths_dim_insert)
    conn.commit()

    logging.info('Loading bing_dim table.')
    load_bing_dim(cur, bing_dim_insert)
    conn.commit()

    logging.info('Updating Bing search queries by state and date into bing_dim table.')
    load_bing_dim_queries(cur)
    conn.commit()

    logging.info('Loading covid_fact table.')
    load_covid_fact(cur, covid_fact_insert)
    conn.commit()    

    # close database connection
    conn.close()

    logging.info('ETL pipeline is complete.')


if __name__ == "__main__":
    main()
