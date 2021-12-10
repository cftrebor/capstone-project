import glob
import logging
import pandas as pd
import sys
import time
from sql_queries import *

state_abbrv_dict = {
     'Alabama': 'AL'
    ,'Alaska': 'AK'
    ,'Arizona': 'AZ'
    ,'Arkansas': 'AR'
    ,'California': 'CA'
    ,'Colorado': 'CO'
    ,'Connecticut': 'CT'
    ,'Delaware': 'DE'
    ,'Florida': 'FL'
    ,'Georgia': 'GA'
    ,'Hawaii': 'HI'
    ,'Idaho': 'ID'
    ,'Illinois': 'IL'
    ,'Indiana': 'IN'
    ,'Iowa': 'IA'
    ,'Kansas': 'KS'
    ,'Kentucky': 'KY'
    ,'Louisiana': 'LA'   
    ,'Maine': 'ME'
    ,'Maryland': 'MD'
    ,'Massachusetts': 'MA'
    ,'Michigan': 'MI'
    ,'Minnesota': 'MN'
    ,'Mississippi': 'MS'
    ,'Missouri': 'MO'
    ,'Montana': 'MT'
    ,'Nebraska': 'NE'
    ,'Nevada': 'NV'
    ,'New Hampshire': 'NH'
    ,'New Jersey': 'NJ'
    ,'New Mexico': 'NM'
    ,'New York': 'NY'
    ,'North Carolina': 'NC'   
    ,'North Dakota': 'ND'
    ,'Ohio': 'OH'
    ,'Oklahoma': 'OK'
    ,'Oregon': 'OR'
    ,'Pennsylvania': 'PA'
    ,'Rhode Island': 'RI'
    ,'South Carolina': 'SC'
    ,'South Dakota': 'SD'
    ,'Tennessee': 'TN'
    ,'Texas': 'TX'
    ,'Utah': 'UT'
    ,'Vermont': 'VT'
    ,'Virginia': 'VA'
    ,'Washington': 'WA'
    ,'West Virginia': 'WV'   
    ,'Wisconsin': 'WI'
    ,'Wyoming': 'WY'
}


def process_vacc_data(source, destination):
    """
    Process source COVID-19 vaccination data
    """
    logging.info("Processing source COVID-19 vaccination data.")
    vacc_df = pd.read_csv(source)

    # remove non-state records from dataframe
    logging.info("Removing non-state records from vaccination dataframe.")
    for each in ('AS', 'BP2', 'DD2', 'FM', 'GU', 'IH2', 'LTC', 'MH', 'MP', 'PR', 'RP', 'VA2', 'VI', 'US', 'DC'):
        vacc_df.drop(vacc_df.index[vacc_df['Location'] == each], inplace=True)

    # replace NaNs with default values for dtype
    char_cols = ['Location']
    date_cols = ['Date']
    integer_cols = list(vacc_df.columns)
    integer_cols.remove('Location')
    integer_cols.remove('Date')

    # apply default value for date columns
    for col in date_cols:
        nans = vacc_df[col].isna().sum()
        if nans > 0:   
            vacc_df[col].fillna('0001-01-01', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # apply default value for integer columns            
    for col in integer_cols:
        nans = vacc_df[col].isna().sum()
        if nans > 0: 
            vacc_df[col].fillna(-1, inplace=True)
            logging.info(f'NaNs were replaced for column {col}')

    # apply default value for character columns             
    for col in char_cols:
        nans = vacc_df[col].isna().sum()
        if nans > 0: 
            vacc_df[col].fillna(' ', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # get total NaN count after NaN processing            
    # if total NaNs > 0 exit program
    total_nans = vacc_df.isna().sum().sum()
    if total_nans > 0:
        logging.error('NaNs still exist.')
        sys.exit()
    else:
        logging.info('All NaNs have been removed from the dataframe.')

    # save final dataframe to csv file in processed data directory
    logging.info("Saving processed vaccination dataframe to csv file.")
    vacc_df.to_csv(f'{destination}/vacc_df.csv', sep='|', index=False, header=False)


def process_cases_death_data(source, destination):
    """
    Process source COVID-19 cases and death data
    """
    logging.info("Processing source COVID-19 case and death data.")
    cases_death_df = pd.read_csv(source)

    # remove non-state records from dataframe
    logging.info("Removing non-state records from cases and death dataframe.")
    for each in ('AS', 'DC', 'FSM', 'GU', 'MP', 'PR', 'PW', 'RMI', 'VI', 'NYC'):
        cases_death_df.drop(cases_death_df.index[cases_death_df['state'] == each], inplace=True)

    # replace NaNs with default values for dtype
    logging.info("Removing NaNs from cases and death dataframe.")
    
    char_cols = ['state','consent_cases','consent_deaths']
    date_cols = ['submission_date','created_at']
    integer_cols = ['tot_cases','conf_cases','prob_cases','new_case','pnew_case','tot_death','conf_death','prob_death','new_death','pnew_death']

    # apply default value for date columns
    for col in date_cols:
        nans = cases_death_df[col].isna().sum()
        if nans > 0:   
            cases_death_df[col].fillna('0001-01-01', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # apply default value for integer columns            
    for col in integer_cols:
        nans = cases_death_df[col].isna().sum()
        if nans > 0: 
            cases_death_df[col].fillna(-1, inplace=True)
            logging.info(f'NaNs were replaced for column {col}')

    # apply default value for character columns             
    for col in char_cols:
        nans = cases_death_df[col].isna().sum()
        if nans > 0: 
            cases_death_df[col].fillna(' ', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # get total NaN count after NaN processing            
    # if total NaNs > 0 exit program
    total_nans = cases_death_df.isna().sum().sum()
    if total_nans > 0:
        logging.error('NaNs still exist.')
        sys.exit()
    else:
        logging.info('All NaNs have been removed from the dataframe.')

    # save final dataframe to csv file in processed data directory
    logging.info("Saving processed cases and death dataframe to csv file.")
    cases_death_df.to_csv(f'{destination}/cases_deaths_df.csv', sep='|', index=False, header=False)


def process_bing_search_data(source, destination):
    """
    Process source COVID-19 Bing search date
    """
    logging.info("Processing source COVID-19 Bing search data.")
    bing_df = pd.concat(map(pd.read_table, glob.glob(source)))

    # remove non-US records
    logging.info("Removing non-US records from Bing search dataframe.")
    us_df = bing_df[bing_df['Country'] == 'United States']

    # removing Washington DC records from dataframe
    logging.info("Removing Washington DC from Bing search dataframe.")
    us_df.drop(us_df.index[us_df['State'] == "District Of Columbia"], inplace=True)

    # replace NaNs with default values for dtype
    char_cols = ['Query','State','Country']
    date_cols = ['Date']
    integer_cols = ['PopularityScore']
    
    # apply default value for date columns
    for col in date_cols:
        nans = us_df[col].isna().sum()
        if nans > 0:   
            us_df[col].fillna('0001-01-01', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # apply default value for integer columns            
    for col in integer_cols:
        nans = us_df[col].isna().sum()
        if nans > 0: 
            us_df[col].fillna(-1, inplace=True)
            logging.info(f'NaNs were replaced for column {col}')

    # apply default value for character columns             
    for col in char_cols:
        nans = us_df[col].isna().sum()
        if nans > 0: 
            us_df[col].fillna(' ', inplace=True)
            logging.info(f'NaNs were replaced for column {col}')
            
    # get total NaN count after NaN processing            
    # if total NaNs > 0 exit program
    total_nans = us_df.isna().sum().sum()
    if total_nans > 0:
        logging.error('NaNs still exist.')
        sys.exit()
    else:
        logging.info('All NaNs have been removed from the dataframe.')

    # add state abbreviation column to dataframe based on State value
    logging.info("Populating new state abbreviation column on Bing search dataframe.")
    us_df['State_Abbrv'] = us_df['State'].map(state_abbrv_dict)

    # save final dataframe to csv file in processed data directory
    logging.info("Saving processed Bing search dataframe to csv file.")
    us_df.to_csv(f'{destination}/bing_search_df.csv', columns=['Date', 'Query', 'State', 'Country', 'PopularityScore', 'State_Abbrv'], sep='|', index=False, header=False)


def main():
    """
    Process source data then store the cleansed/processed datasets to the processed_data directory in csv format.  
    """
    source = './source_data'
    destination = './processed_data'

    # configure log file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(filename=f'./logs/process-data-{timestr}.log', encoding='utf-8', level=logging.INFO, format='%(asctime)s %(levelname)-4s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # process COVID-19 vaccine data
    logging.info("Processing COVID-19 vaccination data.")
    process_vacc_data(f'{source}/COVID-19_Vaccinations_in_the_United_States_Jurisdiction.csv', destination)

    # process COVID-19 case and death data
    logging.info("Processing COVID-19 case and death data.")
    process_cases_death_data(f'{source}/United_States_COVID-19_Cases_and_Deaths_by_State_over_Time.csv', destination)

    # process COVID-19 related Bing search data
    logging.info("Processing Bing search data.")
    process_bing_search_data(f'{source}/Bing/*.tsv', destination)

    logging.info(f'Data processing steps are complete. Datasets saved to: {destination}')


if __name__ == "__main__":
    main()
