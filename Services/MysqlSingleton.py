import peewee
import datetime
from peewee import *
from dotenv import load_dotenv
import os

load_dotenv()

db=None

def getDBConnection():
    print("getting connection")
    if db == None:
        db = MySQLDatabase(os.getenv("MYSQL_DBNAME"), 
                            host=os.getenv("MYSQL_HOST"),
                            port=int(os.getenv("MYSQL_PORT")), 
                            user=os.getenv("MYSQL_USER"), 
                            password=os.getenv("MYSQL_PASSWORD"))
    return db