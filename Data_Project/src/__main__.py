import logging
from paths import path
from data_reading import DataReading
from files import FileName
import os
from data_analysis import Analysis
from data_injection import DataInjection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(path.get_logs_path(), "data_analysis_main.log")
        ),
        logging.StreamHandler(),
    ],
)


def main():
    logging.info("Starting the main function")
    try:
        # reading the data
        data_frames = DataReading.read_data(FileName.get_files_name())
        for name, data_frame in data_frames.items():
            #processing the data
            if data_frame is not None:
                logging.info(f"Reading the data {name} is successful")
                if name == "country_wise":
                    Analysis.country_wise_eda(data_frame=data_frame, name=name)
                elif name == "covid_19_clean":
                    Analysis.covid_19_clean_eda(data_frame == data_frame, name=name)
                elif name == "day_wise":
                    Analysis.day_wise_eda(data_frame=data_frame, name=name)
                elif name == "full_grouped":
                    Analysis.full_grouped_eda(data_frame=data_frame, name=name)
                elif name == "usa_county_wise":
                    Analysis.usa_county_wise_eda(data_frame=data_frame, name=name)
                elif name == "worldometer":
                    Analysis.worldometer_eda(data_frame=data_frame, name=name)
                else:
                    logging.info(
                        f"Data processing for {name} is not defined in the main function"
                    )

            else:
                logging.info(
                    f"Data of {name} doesnot exist or there is a problem with the data"
                )
        
        try:
         #injecting the data to db
         db_create=DataInjection(db_name="covid_19_db")
         db_create.create_database()
         db_create.inject_data_from_parquet()
         
        except Exception as e:
         logging.error(f"error occured durin the data injection: {e}")
        
        finally:
            if db_create:
                db_create.close_connection()


    except Exception as e:
        logging.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
