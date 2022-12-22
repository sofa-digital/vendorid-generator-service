import json, requests
import os
from dotenv import load_dotenv
load_dotenv()

def getToken():
    
    url = os.getenv("url_molten")
    payload = {
        "identifier": os.getenv("identifier"),
        "password": os.getenv("password")
    }
    print(payload)

    response = requests.post(url, json = payload)
    token=response.headers['set-cookie'].split('; Domain')[0]
    token=token.split('Authorization=')[1]
    print("token==",token)
    return token

def update_molten(molten_id:str, vendor_id:str):
    print("7.1):updating molten API")
    url_update_molten = os.getenv("url_update_molten")+molten_id
    payload = {"document":
        {"userCreatedFields":
          {"vendor_id": vendor_id}
        }
    }
    print(payload)

    try:
        token=getToken()
        result = requests.post(url_update_molten,
                        headers={
                            'Authorization':'Bearer ' + token,
                            'Content-Type': 'application/json'
                        }, json = payload)
             
        if result.status_code in [201,200]:
            print("Molten Updated")
            print(result.text)
            return True
        else:
            print("Error",result.text)
            return False
    except Exception as e:
        print("ERROR")
        print(str(e))
        return False
