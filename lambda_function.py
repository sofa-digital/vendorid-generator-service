import json, requests
from Models.ContentRequest import ContentRequest
import os
from dotenv import load_dotenv
from Models.DataLoad import DataLoadResponse
from Models.DataLoadLOGRequest import DataLoadLOGRequest
import time


from Models.TitleRequest import TitleRequest
load_dotenv()


test = False


def create_data_load(titleRequest:TitleRequest):
    time.sleep(0.5)
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
    
def generate_vendor_id(titleRequest:TitleRequest):
    time.sleep(0.5)
    print("@generate_vendor_id")
    url_generate_vendor_id = os.getenv("url_generate_vendor_id")
    payload = {	
        'moltenId':f"{titleRequest.moltenId}",
        'title':f"{titleRequest.title}"
    }

    print("\n\n:::BUILT PAYLOAD:::\n\n")
    print(payload)

    try:
        x = requests.post(url_generate_vendor_id, json = payload)        
        if x.status_code in [201,200]:
            print("Vendor ID Generated")
            print(x.text)
            return True
        else:
            print("Error",x.text)
            return False
    except Exception as e:
        print("ERROR")
        print(str(e))
        return False

def create_log(log:DataLoadLOGRequest):
    time.sleep(0.5)
    print("@create_log")
    url_generate_vendor_id = os.getenv("url_create_log")
    payload = {	
        'dataLoadId':f"{log.dataLoadId}",
        'routine':f"{log.routineId}",
        'state':f"{log.stateId}",
        'status':f"{log.statusId}",
        'detail':f"{log.detail}"
    }

    print("\n\n:::BUILT PAYLOAD:::\n\n")
    print(payload)

    try:
        x = requests.post(url_generate_vendor_id, json = payload)        
        if x.status_code in [201,200]:
            print("LOG Registered")
            print(x.text)
            return True
        else:
            print("Error",x.text)
            return False
    except Exception as e:
        print("ERROR")
        print(str(e))
        return False

def lambda_handler(event, context): 
    try:   
        for record in event['Records']:        
            #1)getting content
            print(":::getting content:::")
            payload = record["body"]
            content = TitleRequest()
            content.__dict__ = json.loads(payload)       
            print(content.__dict__)

            #2)creating dataload
            print(":::creating dataload:::")
            dataLoad = create_data_load(content)
            
            log = DataLoadLOGRequest()
            log.dataLoadId = dataLoad.id
            log.stateId = os.getenv("STATE_PROCESSING")
            log.statusId = os.getenv("STATUS_SUCCESS")
            log.routineId = os.getenv("routine")
            log.detail = "Generating Vendor ID"

            create_log(log)

            #3)generating vendor_id 
            print(":::generating vendor_id :::")
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

                create_log(log)
            else:
                log = DataLoadLOGRequest()
                log.dataLoadId = dataLoad.id
                log.stateId = os.getenv("STATE_FINISHED")
                log.statusId = os.getenv("STATUS_ERROR")
                log.routineId = os.getenv("routine")
                log.detail = "Error Generating Vendor ID"

                create_log(log)


    except Exception as e:
        print(":::::ERROR:::::")
        print(str(e))
        

if test:
    ####testing local####
    testContent = TitleRequest()
    testContent.title ="Filme do Caverna Dragao"
    testContent.moltenId='999'

    dataLoad = create_data_load(testContent)
    

    log = DataLoadLOGRequest()
    log.dataLoadId = dataLoad.id
    log.stateId = os.getenv("STATE_PROCESSING")
    log.statusId = os.getenv("STATUS_SUCCESS")
    log.routineId = os.getenv("routine")
    log.detail = "Generating Vendor ID"

    create_log(log)
    

    isGenerated = generate_vendor_id(testContent)

    log = DataLoadLOGRequest()
    log.dataLoadId = dataLoad.id
    log.stateId = os.getenv("STATE_FINISHED")
    log.statusId = os.getenv("STATUS_SUCCESS")
    log.routineId = os.getenv("routine")
    log.detail = "Vendor ID Generated"

    create_log(log)