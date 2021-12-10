# DEA Capstone Write up by Robert Cramer

## Step 1: Scope the project and gather data

### Data Sources

* __Data sources__

	1. COVID-19 Bing Search data by state
		[link](https://github.com/microsoft/BingCoronavirusQuerySet)
	2. COVID-19 Cases and Deaths data
		[link](https://data.cdc.gov/Case-Surveillance/United-States-COVID-19-Cases-and-Deaths-by-State-o/9mfq-cb36)
	3. COVID-19 Vaccination data
		[link](https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-Jurisdi/unsk-b7fc)

* __Use cases__

	This pipeline will be used to perform analysis of COVID-19 data and demonstrate COVID-19 historical vaccination and case/death trends and any possible correlations such as affect of vaccinations on COVID-19 cases/deaths. Bing search data has also been integrated to see if and how Bing searches related to COVID-19 changed over time.

## Step 2: Explore and assess the data

* __Cleaning the data__

	The datasets used for this project did not include duplicate values that had to be handled. Missing values were the biggest problem with the data. To handle the missing values a function was created to default these values with respect to the dtype of the pandas column. Date columns defaulted to 0001-01-01, strings and objects were defaulted to a space character, and float and integer columns defaulted to -1.

	All datasets included records from all over the world not just the United States. All non-US records were removed from the datasets and the New York City specific records were removed from the cases and death data as these numbers were included in the New York state data. The Bing search data did not include a state abbreviation column so this column was created and the data populated by using a state abbreviation dictionary defined in the ETL code. This state abbreviation addition allows joining the Bing search data to other database tables that include this information.

## Step 3: Define data model

Date and location values are utilized to relate the data records from the disparate COVID-19 datasources.

__Data pipeline steps:__

__Note:__ All Python scripts save output to a log file in __logs__ directory.

1. Update __config__ file to add appropriate values for destination databases and AWS credentials and s3 bucket.
2. Run __process_source_data.py__ to cleanse/process datasets stored in __source_data__ directory. The output datasets are saved to the __processed_data__ directory.
3. Run __upload_datasets_to_s3.py__ to upload the output datasets in __processed_data__ directory to s3 bucket.

4. Run __create_staging_tables.py__ to create staging tables in destination Redshift database.

5. Run __create_wh_tables.py__ to create warehouse tables in destination Redshift database.

6. Run __etl.py__ to load staging tables from s3 datasets and then load dimension and fact tables using SQL against staging tables.

## Step 4: Run ETL to model data

... data dictionaries ...

## Step 5: Complete project write up

1. Goal

	Apache Spark isn’t really needed for this data pipeline as the pipeline does not require a real-time big data analytics and is not aggregating data from sources outside a DBMS such as S3.  
	
	Airflow would definitely be incorporated to automate and schedule the data pipeline so
	manual intervention is not needed in order to execute the pipeline. Airflow would also allow 
	the data pipeline to be integrated into the team’s incident notification tool.

2. Rationale

	AWS Redshift was chosen as the database since the data is coming from disparate sources and is relational in nature based on relation of dates and locations. I chose not to use S3 for the stage data as I found it useful in this case to have relational database functionality to process new/future records and also because Spark was not needed to process the staged data.

3. Steps of process

	This is documented in the __Data pipeline steps__ section under __Step 3__.

4. Propose frequency of pipeline execution

	My proposition of pipeline execution frequency would depend on the needs of the 
	downstream customer and the frequency that the data at the source was updated. I believe it 
	would be reasonable to update this data weekly to balance prevention of not having current 
	enough data versus the utilization of computing resources to process and store the data for 
	querying by end users.

5. GitHub Repository

	[https://github.com/cftrebor/capstone-project](https://github.com/cftrebor/capstone-project)

6. How would you approach the problem differently under the following scenarios:


* __Data was increased by 100x:__

	I would break out the staging tables into a new Redshift cluster so processing for staging does not impact warehouse processing against fact/dimension tables.

	I would also break up the data into smaller pieces and use S3 to store this data and implement Spark to process the greater amount of data in a parallel distributed fashion to prevent data processing from running past SLA or an acceptable amount of time.
              
* __Pipelines were run on a daily basis by 7:00 am:__

	I would break up the datasets to process only data records created/updated since the previous run of the pipeline and not always process the full datasets. 

	I would also ensure to implement Airflow to schedule/automate the data pipeline and enforce the 7:00 AM SLA.

* __Database needed to be accessed by 100+ people:__

	I would break out the staging tables into a new Redshift cluster so processing for staging does not impact warehouse processing against fact/dimension tables.

	I would  also closely analyze database table partitioning, sorting, and indexes to maximize efficiency and concurrency at the database level. I would also review all SQL associated with the pipeline to ensure each query performed as efficiently as possible and minimal sorts were being performed.