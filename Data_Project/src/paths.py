import os

class path:
    @staticmethod
    def get_project_root():
        return os.path.dirname(os.path.dirname(__file__)) 

    def get_db_files_path():
        return os.path.join(path.get_project_root(), 'data','db_files')
        
    
    @staticmethod
    def get_intermediate_path():
        return os.path.join(path.get_project_root(), 'data', 'intermediate')
    
    @staticmethod
    def get_input_path():
        return os.path.join(path.get_project_root(), 'data', 'input')
    
    @staticmethod
    def get_output_path():
        return os.path.join(path.get_project_root(), 'data', 'output')
    
    @staticmethod
    def get_logs_path():
        return os.path.join(path.get_project_root(), 'logs')
    
    @staticmethod
    def get_src_path():
        return os.path.join(path.get_project_root(), 'src')



