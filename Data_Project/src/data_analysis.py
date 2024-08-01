import os
import logging
from paths import path
import pandas as pd
from data_validations import Validate
from data_conversion import DataConvert

# Configure logging
logging.basicConfig(    
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(path.get_logs_path(), "data_analysis.log")),
        logging.StreamHandler(),
    ],
)


class Analysis:

    @staticmethod
    def country_wise_eda(data_frame: pd.DataFrame, name: str):
        logging.info("==============Starting Country_wise Data analysis==============")
        logging.info("")
        try:

            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={
                    "Country/Region": "country_region",
                    "Confirmed": "confirmed",
                    "Deaths": "deaths",
                    "Recovered": "recovered",
                    "Active": "active",
                    "New cases": "new_cases",
                    "New deaths": "new_deaths",
                    "New recovered": "new_recovered",
                    "Deaths / 100 Cases": "deaths_per_100_cases",
                    "Recovered / 100 Cases": "recovered_per_100_cases",
                    "Deaths / 100 Recovered": "deaths_per_100_recovered",
                    "Confirmed last week": "confirmed_last_week",
                    "1 week change": "one_week_change",
                    "1 week % increase": "one_week_percent_increase",
                    "WHO Region": "who_region",
                },
                inplace=True,
            )

            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")

            # Check for null values
            null_values_sum = Validate.check_null_sum(df)
            if null_values_sum == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info("Null values found, processing the null values")
                df = df.dropna()
                logging.info("Done")

            # Validate column values and types
            df = pd.DataFrame(
                Validate.column_values_between(
                    {name: df}, "deaths", 0, 81985000, path.get_intermediate_path()
                )
            )
            
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},["one_week_change","one_week_percent_increase"])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in country_wise_eda: {e}")
    
    @staticmethod
    def covid_19_clean_eda(data_frame: pd.DataFrame,name: str):
        logging.info("==============Starting Covid_19_clean Eda analysis==============")
        logging.info("")
        try:
            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={
                    'Province/State': 'province_state',
                    'Country/Region': 'country_region',
                    'Lat': 'lat','Long': 'long',
                    'Date': 'Date',
                    'Confirmed': 'confirmed',
                    'Deaths': 'deaths',
                    'Recovered': 'recovered',
                    'Active': 'active',
                    'WHO Region': 'who_region'},inplace=True)
            
            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")

            # Check for null values
            df=df['province_state'].fillna('Unknown', inplace=True)
            null_cols_list = Validate.check_null_get_col_list(df)
            if len(null_cols_list) == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info(f"Null values found in {null_cols_list}")
                logging.info("processing the null values")
                df=Validate.fill_null_values(df,{null_cols_list:"unknown"})
                logging.info(f"processing done.... null values processed as unkmown")

                     
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},['confirmed', 'deaths', 'recovered', 'active'])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in the Covid_19_clean_eda: {e}")
    
    @staticmethod
    def day_wise_eda(data_frame: pd.DataFrame,name: str):

        logging.info("==============Starting day_wise Eda analysis==============")
        logging.info("")
        try:
            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={'Date': 'date',
                        'Confirmed': 'confirmed',
                        'Deaths': 'deaths',
                        'Recovered': 'recovered',
                        'Active': 'active',
                        'New cases': 'new_cases',
                        'New deaths': 'new_deaths',
                        'New recovered': 'new_recovered',
                        'Deaths / 100 Cases': 'deaths_per_100_cases',
                        'Recovered / 100 Cases': 'recovered_per_100_cases',
                        'Deaths / 100 Recovered': 'deaths_per_100_recovered',
                        'No. of countries': 'no_of_countries',},inplace=True)
            
            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")

            # Check for null values
            null_cols_list = Validate.check_null_get_col_list(df)
            if len(null_cols_list) == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info(f"Null values found in {null_cols_list}")
                logging.info("processing the null values")
                df=Validate.fill_null_values(df,{null_cols_list:"unknown"})
                logging.info(f"processing done.... null values processed ")

                     
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},['confirmed', 'deaths', 
                                                          'recovered', 'active', 
                                                          'new_cases', 'new_deaths', 
                                                          'new_recovered', 
                                                          'deaths_per_100_cases', 
                                                          'recovered_per_100_cases',
                                                          'deaths_per_100_recovered',
                                                          'no_of_countries'])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in the day_wise_eda(): {e}")
    
    @staticmethod
    def full_grouped_eda(data_frame: pd.DataFrame,name: str):
    
        logging.info("==============Starting full_grouped Eda analysis==============")
        logging.info("")
        try:
            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={'Date': 'date',
                        'Province/State': 'province_state',
                        'Country/Region': 'country_region',
                        'Confirmed': 'confirmed',
                        'Deaths': 'deaths',
                        'Recovered': 'recovered',
                        'Active': 'active',
                        'New cases': 'new_cases',
                        'New deaths': 'new_deaths',
                        'New recovered': 'new_recovered',
                        'Who Region': 'who_region',},inplace=True)
            
            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")

            # Check for null values
            null_cols_list = Validate.check_null_get_col_list(df)
            if len(null_cols_list) == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info(f"Null values found in {null_cols_list}")
                logging.info("processing the null values")
                df=Validate.fill_null_values(df,{null_cols_list:"unknown"})
                logging.info(f"processing done.... null values processed ")

                     
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},['confirmed', 'deaths', 'recovered', 'active',
       'new_cases', 'new_deaths', 'new_recovered',])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in the full_grouped_eda(): {e}")
    
    @staticmethod
    def usa_county_wise_eda(data_frame: pd.DataFrame,name: str):
        logging.info("==============Starting usa_county_wise Eda analysis==============")
        logging.info("")
        try:
            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={'Province_State': 'province_state',
                        'Country_Region': 'country_region',
                        'Admin2': 'admin2',
                        'Admin3': 'admin3',
                        'Admin4': 'admin4',
                        'FIPS': 'fips',
                        'Combined_Key': 'combined_key',
                        'Lat': 'lat',
                        'Long_': 'long',
                        'Confirmed': 'confirmed',
                        'Deaths': 'deaths',
                        'Recovered': 'recovered',
                        'Active': 'active',
                        'Combined_Key': 'combined_key'},inplace=True)
            
            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")
            # Check for null values
            null_cols_list = Validate.check_null_get_col_list(df)
            if len(null_cols_list) == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info(f"Null values found in {null_cols_list}")
                logging.info("processing the null values")
                df=Validate.fill_null_values(df,{"fips":0,"admin2":"unknown",})
                logging.info(f"processing done.... null values processed ")

                     
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},['deaths'])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in the usa_county_wise_eda(): {e}")
    
    @staticmethod
    def worldometer_eda(data_frame: pd.DataFrame,name: str):
        logging.info("==============Starting worldometer Eda analysis==============")
        logging.info("")
        try:
            df = data_frame
            logging.info(f"Dimensions of data: {df.shape}")
            logging.info("Changing the column names to the appropriate terms")

            # Rename columns
            df.rename(
                columns={'Country/Region': 'country_region',
                        'WHO Region': 'who_region',
                        'Confirmed': 'confirmed',
                        'Continent': 'continent',
                        'Population': 'population',
                        'TotalCases': 'total_cases',
                        'Deaths': 'deaths',
                        'Recovered': 'recovered',
                        'Active': 'active',
                        'NewCases':'new_cases',
                        'TotalDeaths':'total_deaths',
                        'NewDeaths':'new_deaths',
                        "TotalRecovered":"total_recovered",
                        'NewRecovered'   :'new_recovered',
                        'ActiveCases':'active_cases',
                        'Serious,Critical':'serious_critical', 
                        'Tot Cases/1M pop':'total_cases_per_million', 
                        'Deaths/1M pop':'deaths_per_million',
                        'TotalTests':'total_tests', 
                        'Tests/1M pop':'tests_per_million', 
                        'WHO Region':'who_region'},inplace=True)
            
            logging.info("Column names changed successfully")
            logging.info("Checking for null values in the data")
           

            # Check for null values
            null_cols_list = Validate.check_null_get_col_list(df)
            if len(null_cols_list) == 0:
                logging.info("No null values found, processing further...")
            else:
                logging.info(f"Null values found in {null_cols_list}")
                logging.info("processing the null values")
                df=Validate.fill_null_values(df,{'population': 0,
                                                'total_cases': 0,
                                                'new_cases': 0,
                                                'total_deaths': 0,
                                                'new_deaths': 0,
                                                'total_recovered': 0,
                                                'new_recovered': 0,
                                                'active_cases': 0,
                                                'serious_critical': 0,
                                                'total_cases_per_million': 0,
                                                'deaths_per_million': 0,
                                                'total_tests': 0,
                                                'tests_per_million': 0,
                                                'continent':"Unknown",
                                                'who_region':"Unknown",})
                logging.info(f"processing done.... null values processed ")

                     
            logging.info("checking for negative values which are not needed")
            df=Validate.column_values_negative({name:df},['population', 
                                                          'total_cases', 
                                                          'new_cases',
                                                          'total_deaths', 
                                                          'new_deaths', 
                                                          'total_recovered', 
                                                          'new_recovered',
                                                          'active_cases'])
            logging.info("Preprocessing completed, converting to parquet format")
            DataConvert.to_parquet({name: df}, path.get_output_path())
        except Exception as e:
            logging.error(f"Error in the worldometer_eda(): {e}")


        
    
    