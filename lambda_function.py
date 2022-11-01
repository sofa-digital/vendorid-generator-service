import json, requests

class ContentRequest:
    body:None
    routine:None
    dataLoadType:None


def lambda_handler(event, context):    
    for record in event['Records']:        
        payload = record["body"]
        content = ContentRequest()
        content.__dict__ = json.loads(payload)
        

        url = 'http://suppliers-load-balance-5b7d478e27b7c043.elb.sa-east-1.amazonaws.com:8003/create-data-load'
        data = content.body
        print(data)
        dataOk={'body': str(data).replace("'","\""),'routine':'2','dataLoadType':'1'}
        print(dataOk)
        try:
            x = requests.post(url, json = dataOk)        
            print(x.text)
            print("SUCCESS")

            urlVodCore = "http://suppliers-load-balance-5b7d478e27b7c043.elb.sa-east-1.amazonaws.com:8004/generate-vendorid"
            dataVodCore = {	
                            'moltenId':'1231',
                            'title':'test-allan'
                            }

        except Exception as e:
            print("ERROR")
            print(e)

