import calendar
import json, requests
from Models.ContentRequest import ContentRequest
import os
from dotenv import load_dotenv
from Models.DataLoad import DataLoadResponse
from Models.DataLoadLOGRequest import DataLoadLOGRequest
import time
from peewee import *
import peewee
import datetime

from Models.TitleRequest import TitleRequest

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
import uuid

from Services.MoltenUpdater import update_molten

load_dotenv()

test = False

if not test:
    patch_all()

db = MySQLDatabase(os.getenv("MYSQL_DBNAME"), 
                            host=os.getenv("MYSQL_HOST"),
                            port=int(os.getenv("MYSQL_PORT")), 
                            user=os.getenv("MYSQL_USER"), 
                            password=os.getenv("MYSQL_PASSWORD"))

def create_data_load(titleRequest:TitleRequest):
    
    if test:
        print("###### T E S T  M O D E ######")
    print("@create_data_load")
    url_create_data_load = os.getenv("url_create_data_load")    
    print(":::::url_create_data_load:::::")
    print(url_create_data_load)

    payload={
        'body': str(titleRequest.__dict__).replace("'","\""),
        'routine':os.getenv("routine"),
        'dataLoadType':os.getenv("data_load_type")
    }
    print("\n\n:::BUILT PAYLOAD:::\n\n")
    print(payload)
    print("\n::::::")

    x = requests.post(url_create_data_load, json = payload)        
    if x.status_code == 201:
        print("Data Load Created")
        
        createdDataLoad = DataLoadResponse()
        createdDataLoad.id = x.json()["id"]
        return createdDataLoad
    else:
        print("Error",x.text)
        return None
    
class TitleModel(peewee.Model):
    id = peewee.CharField()
    molten_id = peewee.CharField()
    title = peewee.CharField()
    vendor_id = peewee.CharField()
 
    class Meta:
        database = db
        db_table = 'title'

class VendorIDRepository():
    def __init__(self):
        print("1) constructor VendorIDRepository")
      
    def create(self, obj): 
        print("5) verifying if exists")
        exists = TitleModel.select().where(TitleModel.molten_id == obj.moltenId)
        if len(exists) > 0:
            print("===> item already exists")
            return None
        print("6) creating in database")
        date = datetime.datetime.utcnow()
        utc_time = calendar.timegm(date.utctimetuple())   
        generatedVendorID = obj.title.upper().replace(" ", "").replace("-", "")+ \
            "_SOFA_"+ \
            datetime.datetime.now().strftime("%Y%m%d")+ \
            "_"+str(utc_time)
        newid=uuid.uuid4()
        newTitleCore = TitleModel.create(                
            molten_id  = obj.moltenId,
            title  = obj.title,
            vendor_id = generatedVendorID,
            id = str(newid)
        )

        print("7) updating in molten ",obj.moltenId,generatedVendorID)
        update_molten(obj.moltenId,generatedVendorID)
        #update_molten("63986c2de655a4bc64c09f9e","TESTE")
        
        newTitleCore.save()

def generate_vendor_id(titleRequest:TitleRequest):
    
    print("4)generate vendor id 1855")

    try:
        
        print("4)calling vendorIDRepository.create", titleRequest)
        vendorIDRepository = VendorIDRepository()
        vendorIDRepository.create(titleRequest)
        
        return True
    except Exception as e:
        print("ERROR")
        print(str(e))
        return False

def create_log(log:DataLoadLOGRequest, step:str):
    
    print("##creating log####", step)
    url_create_log = os.getenv("url_create_log")
    print(url_create_log)
    payload = {	
        'dataLoadId':f"{log.dataLoadId}",
        'routine':f"{log.routineId}",
        'state':f"{log.stateId}",
        'status':f"{log.statusId}",
        'detail':f"{log.detail}"
    }

    print("\n\n:::BUILT PAYLOAD:::\n\n")
    print(payload)

    
    x = requests.post(url_create_log, json = payload)        
    if x.status_code in [201,200]:
        print("LOG Registered")
        print(x.text)        
    else:
        print("Error",x.text)        


def lambda_handler(event, context): 
 
    db = MySQLDatabase(os.getenv("MYSQL_DBNAME"), 
                                host=os.getenv("MYSQL_HOST"),
                                port=int(os.getenv("MYSQL_PORT")), 
                                user=os.getenv("MYSQL_USER"), 
                                password=os.getenv("MYSQL_PASSWORD"))
    print(":::INFO total records:::")
    print(str(len(event['Records'])))
    try:   
        for record in event['Records']:        
            #1)getting content
            print("1)getting content")
            payload = record["body"]
            content = TitleRequest()
            content.__dict__ = json.loads(payload)       
            print(content.__dict__)

            #2)creating dataload
            print("2)creating dataload")
            dataLoad = create_data_load(content)
            
            log = DataLoadLOGRequest()
            log.dataLoadId = dataLoad.id
            log.stateId = os.getenv("STATE_PROCESSING")
            log.statusId = os.getenv("STATUS_SUCCESS")
            log.routineId = os.getenv("routine")
            log.detail = "Generating Vendor ID"

            create_log(log, "after creating dataload")

            #3)generating vendor_id 
            print("3)generating vendor_id")
            titleRequest = TitleRequest()
            titleRequest.__dict__ = json.loads(payload)  
            isGenerated = generate_vendor_id(titleRequest)

            #4)logging result
            if isGenerated:
                log = DataLoadLOGRequest()
                log.dataLoadId = dataLoad.id
                log.stateId = os.getenv("STATE_FINISHED")
                log.statusId = os.getenv("STATUS_SUCCESS")
                log.routineId = os.getenv("routine")
                log.detail = "Vendor ID Generated"

                create_log(log, "isGenerated = True")

                

            else:
                log = DataLoadLOGRequest()
                log.dataLoadId = dataLoad.id
                log.stateId = os.getenv("STATE_FINISHED")
                log.statusId = os.getenv("STATUS_ERROR")
                log.routineId = os.getenv("routine")
                log.detail = "Error Generating Vendor ID"

                create_log(log, "isGenerated = False")


    except Exception as e:
        print(":::::ERROR:::::")
        print(str(e))



if test:
    ####testing local####
    testContent = TitleRequest()
    testContent.title ="Filme do Caverna Dragao"
    testContent.moltenId='63986c2de655a4bc64c09f9e'

    dataLoad = create_data_load(testContent)
    

    log = DataLoadLOGRequest()
    log.dataLoadId = dataLoad.id
    log.stateId = os.getenv("STATE_PROCESSING")
    log.statusId = os.getenv("STATUS_SUCCESS")
    log.routineId = os.getenv("routine")
    log.detail = "Generating Vendor ID"

    create_log(log, "testing local")
    

    isGenerated = generate_vendor_id(testContent)

    log = DataLoadLOGRequest()
    log.dataLoadId = dataLoad.id
    log.stateId = os.getenv("STATE_FINISHED")
    log.statusId = os.getenv("STATUS_SUCCESS")
    log.routineId = os.getenv("routine")
    log.detail = "Vendor ID Generated"

    create_log(log, "testing local")

