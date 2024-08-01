import os
from dotenv import load_dotenv

load_dotenv()
class Creds:
    @staticmethod
    def  get_Access_Key():
        return os.getenv("Access_Key")
       
    def get_Access_Secret_Key():
        return os.getenv("Access_Secret_Key")
        
    