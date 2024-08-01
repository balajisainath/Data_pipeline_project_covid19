import os

class FileName:
    @staticmethod
    def get_files_name():
        
        project_root = os.path.dirname(os.path.dirname(__file__))
        
        return {
            "country_wise": os.path.join(project_root, 'data', 'input', 'country_wise_latest.csv'),
            "covid_19_clean": os.path.join(project_root, 'data', 'input', 'covid_19_clean_complete.csv'),
            "day_wise": os.path.join(project_root, 'data', 'input', 'day_wise.csv'),
            "full_grouped": os.path.join(project_root, 'data', 'input', 'full_grouped.csv'),
            "usa_county_wise": os.path.join(project_root, 'data', 'input', 'usa_county_wise.csv'),
            "worldometer": os.path.join(project_root, 'data', 'input', 'worldometer_data.csv')
        }
