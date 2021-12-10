# DROP TABLES

vacc_stage_drop = "DROP TABLE IF EXISTS vacc_stage;"
case_death_drop = "DROP TABLE IF EXISTS cases_deaths_by_state_stage;"
bing_search_drop = "DROP TABLE IF EXISTS bing_search_stage;"

vaccine_dim_drop = "DROP TABLE IF EXISTS vaccine_dim;"
cases_deaths_dim_drop = "DROP TABLE IF EXISTS cases_deaths_dim;"
bing_dim_drop = "DROP TABLE IF EXISTS bing_dim;"
covid_fact_drop = "DROP TABLE IF EXISTS covid_fact;"


# CREATE TABLES

vacc_stage_table_create = ("""
CREATE TABLE vacc_stage (
    Date date 
   ,MMWR_week smallint 
   ,Location char(2) 
   ,Distributed real 
   ,Distributed_Janssen real
   ,Distributed_Moderna real
   ,Distributed_Pfizer real
   ,Distributed_Unk_Manuf real
   ,Dist_Per_100K real 
   ,Distributed_Per_100k_12Plus real
   ,Distributed_Per_100k_18Plus real 
   ,Distributed_Per_100k_65Plus real 
   ,Administered real 
   ,Administered_12Plus real 
   ,Administered_18Plus real 
   ,Administered_65Plus real 
   ,Administered_Janssen real 
   ,Administered_Moderna real 
   ,Administered_Pfizer real 
   ,Administered_Unk_Manuf real 
   ,Admin_Per_100K real 
   ,Admin_Per_100k_12Plus real 
   ,Admin_Per_100k_18Plus real 
   ,Admin_Per_100k_65Plus real 
   ,Recip_Administered real 
   ,Administered_Dose1_Recip real 
   ,Administered_Dose1_Pop_Pct real 
   ,Administered_Dose1_Recip_12Plus real 
   ,Administered_Dose1_Recip_12PlusPop_Pct real 
   ,Administered_Dose1_Recip_18Plus real 
   ,Administered_Dose1_Recip_18PlusPop_Pct real 
   ,Administered_Dose1_Recip_65Plus real 
   ,Administered_Dose1_Recip_65PlusPop_Pct real 
   ,Series_Complete_Yes real 
   ,Series_Complete_Pop_Pct real 
   ,Series_Complete_12Plus real 
   ,Series_Complete_12PlusPop_Pct real 
   ,Series_Complete_18Plus real 
   ,Series_Complete_18PlusPop_Pct real 
   ,Series_Complete_65Plus real 
   ,Series_Complete_65PlusPop_Pct real 
   ,Series_Complete_Janssen real 
   ,Series_Complete_Moderna real 
   ,Series_Complete_Pfizer real 
   ,Series_Complete_Unk_Manuf real 
   ,Series_Complete_Janssen_12Plus real 
   ,Series_Complete_Moderna_12Plus real 
   ,Series_Complete_Pfizer_12Plus real 
   ,Series_Complete_Unk_Manuf_12Plus real 
   ,Series_Complete_Janssen_18Plus real 
   ,Series_Complete_Moderna_18Plus real 
   ,Series_Complete_Pfizer_18Plus real 
   ,Series_Complete_Unk_Manuf_18Plus real 
   ,Series_Complete_Janssen_65Plus real 
   ,Series_Complete_Moderna_65Plus real 
   ,Series_Complete_Pfizer_65Plus real 
   ,Series_Complete_Unk_Manuf_65Plus real 
   ,Additional_Doses real 
   ,Additional_Doses_Vax_Pct real 
   ,Additional_Doses_18Plus real 
   ,Additional_Doses_18Plus_Vax_Pct real 
   ,Additional_Doses_50Plus real 
   ,Additional_Doses_50Plus_Vax_Pct real 
   ,Additional_Doses_65Plus real 
   ,Additional_Doses_65Plus_Vax_Pct real 
   ,Additional_Doses_Moderna real 
   ,Additional_Doses_Pfizer real 
   ,Additional_Doses_Janssen real 
   ,Additional_Doses_Unk_Manuf real 
   ,Administered_Dose1_Recip_5Plus real 
   ,Administered_Dose1_Recip_5PlusPop_Pct real 
   ,Series_Complete_5Plus real 
   ,Series_Complete_5PlusPop_Pct real 
   ,Administered_5Plus real 
   ,Admin_Per_100k_5Plus real 
   ,Distributed_Per_100k_5Plus real 
   ,Series_Complete_Moderna_5Plus real 
   ,Series_Complete_Pfizer_5Plus real 
   ,Series_Complete_Janssen_5Plus real 
   ,Series_Complete_Unk_Manuf_5Plus real 
   );
""")

cases_deaths_stage_table_create = ("""
CREATE TABLE cases_deaths_by_state_stage (
    submission_date date 
   ,state char(2)
   ,tot_cases real 
   ,conf_cases real 
   ,prob_cases real 
   ,new_case real 
   ,pnew_case real 
   ,tot_death real 
   ,conf_death real 
   ,prob_death real 
   ,new_death real 
   ,pnew_death real 
   ,created_at timestamp 
   ,consent_cases char(10)
   ,consent_deaths char(10)
);
""")

bing_search_stage_table_create = ("""
CREATE TABLE bing_search_stage (
    date date
   ,query varchar(max)  
   ,State text 
   ,Country text 
   ,PopularityScore integer 
   ,State_Abbr char(2) 
);
""")

vaccine_dim_create = ("""
CREATE TABLE vaccine_dim (
     date date
    ,location char(2)
    ,distributed integer
    ,distributed_janssen integer
    ,distributed_moderna integer
    ,distributed_pfizer integer
    ,distributed_unk_manuf integer
    ,administered integer
    ,administered_janssen integer
    ,administered_moderna integer
    ,administered_pfizer integer
    ,administered_unk_manuf integer
    ,additional_doses integer
    ,additional_doses_moderna integer
    ,additional_doses_pfizer integer
    ,additional_doses_janssen integer
    ,additional_doses_unk_manuf integer
    ,PRIMARY KEY(date, location)
);
""")

cases_deaths_dim_create = ("""
CREATE TABLE cases_deaths_dim (
     submission_date date
    ,state char(2)
    ,tot_cases integer
    ,conf_cases integer
    ,prob_cases integer
    ,new_case integer
    ,tot_death integer
    ,conf_death integer
    ,prob_death integer
    ,new_death integer
    ,PRIMARY KEY(submission_date, state)
);
""")

bing_dim_create = ("""
CREATE TABLE bing_dim (
     date date
    ,state_abbr char(2)
    ,query varchar(max) 
    ,PRIMARY KEY(date, state_abbr)
);
""")

covid_fact_create = ("""
CREATE TABLE covid_fact (
     date date
    ,location char(2)
    ,tot_cases integer
    ,new_case integer
    ,tot_death integer
    ,new_death integer
    ,distributed integer
    ,administered integer
    ,additional_doses integer
    ,bing_search_queries text
    ,PRIMARY KEY(date, location)
);
""")


# INSERT RECORDS

vacc_stage_table_insert = ("""
INSERT INTO vacc_stage (
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
   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

cases_deaths_stage_insert = ("""
INSERT INTO cases_deaths_by_state_stage (
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
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

bing_search_stage_insert = ("""
INSERT INTO bing_search_stage (
    date 
   ,query 
   ,IsImplicitIntent
   ,State
   ,Country
   ,PopularityScore
   ,State_Abbr
) VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

bing_dim_insert = ("""
INSERT INTO bing_dim (date, state_abbr)
    SELECT DISTINCT date, state_abbr
    FROM bing_search_stage;
""")

cases_deaths_dim_insert = ("""
INSERT INTO cases_deaths_dim
  SELECT submission_date
        ,state
        ,tot_cases
        ,conf_cases
        ,prob_cases
        ,new_case
        ,tot_death
        ,conf_death
        ,prob_death
        ,new_death
  FROM cases_deaths_by_state_stage;
""")

covid_fact_insert = ("""
INSERT INTO covid_fact
SELECT 
    c.submission_date
   ,c.state
   ,c.tot_cases
   ,c.new_case
   ,c.tot_death
   ,c.new_death
   ,v.distributed
   ,v.administered
   ,v.additional_doses
   ,b.query
FROM
    cases_deaths_dim c inner join vaccine_dim v
      ON c.submission_date = v.date
      AND c.state = v.location
    inner join bing_dim b
      ON c.submission_date = b.date
      AND c.state = b.state_abbr;
""")

vaccine_dim_insert = ("""
INSERT INTO vaccine_dim
  SELECT date
        ,location
        ,distributed
        ,distributed_janssen
        ,distributed_moderna
        ,distributed_pfizer
        ,distributed_unk_manuf
        ,administered
        ,administered_janssen
        ,administered_moderna
        ,administered_pfizer
        ,administered_unk_manuf
        ,additional_doses
        ,additional_doses_moderna
        ,additional_doses_pfizer
        ,additional_doses_janssen
        ,additional_doses_unk_manuf
  FROM vacc_stage;
""")


# QUERY LISTS

drop_table_queries = [vacc_stage_drop, case_death_drop, bing_search_drop]
create_table_queries = [vacc_stage_table_create, cases_deaths_stage_table_create, bing_search_stage_table_create]

drop_wh_table_queries = [covid_fact_drop, vaccine_dim_drop, cases_deaths_dim_drop, bing_dim_drop]
create_wh_table_queries = [vaccine_dim_create, cases_deaths_dim_create, bing_dim_create, covid_fact_create]



