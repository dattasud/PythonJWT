from os import access
from pathlib import Path
from pydoc import classname
from wsgiref.headers import Headers
import jwt
import json
import requests
from datetime import datetime, timedelta
import time
from jose import jws
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from requests_toolbelt.utils import dump
import pandas as pd
import psycopg2
import psycopg2.extras
import pandas.io.sql as sqlio

classname = 'VendastaPurchases'

def loadPurchaseMetrics_db(purchase_df, since_reqtime, id):
    methodName = "loadPurchaseMetrics_db"    
    db_hostname = ''
    db_port = ''
    db_username = ''
    db_password = ''
    db_database = ''
    if len(purchase_df) > 0:
        try:
            print(classname+"::"+methodName+"::Loading data..")
            myConnection = psycopg2.connect( host=db_hostname, user=db_username, password=db_password, dbname=db_database, port=db_port )
            cur1 = myConnection.cursor()
            leads_query = "DELETE FROM vendasta_purchases_log where iso_8601 = '"+str(since_reqtime)+"' and id = '"+id+"' "
            cur1.execute(leads_query)
            myConnection.commit()
            cur1.close()
            cur = myConnection.cursor()
            df_columns = list(purchase_df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)
            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
            #create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format('vendasta_purchases_log',columns,values)
            cur = myConnection.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, purchase_df.values)
            myConnection.commit()
            print(classname+"::"+methodName+"::Data load completed.")
            cur.close()
            myConnection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(classname+"::DB Error:: ", error)
            return 0
        finally:
            if myConnection is not None:
                myConnection.close()

def loadPurchaselineItemsMetrics_db(purchase_df, since_reqtime, id, customerId, sku):
    methodName = "loadPurchaselineItemsMetrics_db"    
    db_hostname = ''
    db_port = ''
    db_username = ''
    db_password = ''
    db_database = ''
    if len(purchase_df) > 0:
        try:
            print(classname+"::"+methodName+"::Loading data..")
            myConnection = psycopg2.connect( host=db_hostname, user=db_username, password=db_password, dbname=db_database, port=db_port )
            cur1 = myConnection.cursor()
            leads_query = "DELETE FROM vendasta_purchases_lineitems where iso_8601 = '"+str(since_reqtime)+"' and id = '"+id+"' and customerId = '"+customerId+"' and sku = '"+sku+"' "
            cur1.execute(leads_query)
            myConnection.commit()
            cur1.close()
            cur = myConnection.cursor()
            df_columns = list(purchase_df)
            # create (col1,col2,...)
            columns = ",".join(df_columns)
            # create VALUES('%s', '%s",...) one '%s' per column
            values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
            #create INSERT INTO table (columns) VALUES('%s',...)
            insert_stmt = "INSERT INTO {} ({}) {}".format('vendasta_purchases_lineitems',columns,values)
            cur = myConnection.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, purchase_df.values)
            myConnection.commit()
            print(classname+"::"+methodName+"::Data load completed.")
            cur.close()
            myConnection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(classname+"::DB Error:: ", error)
            return 0
        finally:
            if myConnection is not None:
                myConnection.close()

if __name__ == "__main__":
    scope = "profile email financial"
    now_unix = int(time.time())
    exp_time_unix = now_unix+600
    private_key = b"-----BEGIN RSA PRIVATE KEY-----\================\n-----END RSA PRIVATE KEY-----"
    private_key = serialization.load_pem_private_key(private_key, password=None, backend=default_backend())
    header = {"kid": ""}
    cred_dict = {
        "aud": "",
        "iat":  now_unix,
        "exp":  exp_time_unix,
        "iss": "",
        "sub": "",
        "scope": scope
        }
    assertion = jwt.encode(cred_dict, private_key, algorithm="RS256", headers=header)
    header = {'Content-Type': "application/x-www-form-urlencoded"}
    payload = {
        "grant_type":   "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion":    assertion
        }
    token_uri = 'https://sso-api-prod.apigateway.co/oauth2/token'
    turi_response = requests.post(token_uri, data=payload, headers=header)
    response = json.loads(turi_response.text)
    if turi_response.status_code == 200:
        access_token = response['access_token']
        auth_token = "Bearer "+access_token
        #print(response)
        url = ""
        queryDate = str(datetime.today()).split(" ")[0]
        createdAt_from = queryDate+"T04:00:00Z"
        nextDate = str(datetime.today() + timedelta(days=1)).split(" ")[0]
        createdAt_to = nextDate+"T03:59:59Z"
        print(createdAt_from)
        print(createdAt_to)
        querystring = {
            "filter[partner.id]":"",
            "filter[createdAt][>]":createdAt_from,
            "filter[createdAt][<]":createdAt_to,
            "page[limit]":"100"}
        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_token
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        if response.status_code == 200:
            column_names = [ 
            "createdAt",
            "currencyCode" ,
            "discountAmount" ,
            "statusCode" , 
            "taxAmount" , 
            "total" ,
            "id" ,
            "type" ,
            "iso_8601"]
            purchase_df = pd.DataFrame(columns=column_names)
            column_names_lineItem = [ 
            "id",
            "customerId" ,
            "discountAmount" ,
            "quantity" , 
            "sku" , 
            "taxAmount" ,
            "total" ,
            "unitAmount" ,
            "iso_8601"]
            lineItem_df = pd.DataFrame(columns=column_names_lineItem)
            #print("======================================================================")
            #print(response.text)
            with open("response.txt", "w") as f:
                f.write(response.text)
            #print("======================================================================")
            vpurchase_dataload = json.loads(response.text)
            purchases = vpurchase_dataload['data']
            if len(purchases) > 0:
                for purchase in purchases:
                    createdAt = purchase['attributes']['createdAt']
                    currencyCode = purchase['attributes']['currencyCode']
                    discountAmount = purchase['attributes']['discountAmount']
                    statusCode = purchase['attributes']['statusCode']
                    taxAmount = purchase['attributes']['taxAmount']
                    total = purchase['attributes']['total']
                    id = purchase['id']
                    type = purchase['type']
                    datalist = [str(createdAt), str(currencyCode), discountAmount, str(statusCode), taxAmount, total, str(id), str(type), queryDate]
                    length_df = len(purchase_df)
                    purchase_df = purchase_df[0:0]
                    purchase_df.loc[length_df] = datalist
                    print(purchase_df)
                    print("====================================loadPurchaseMetrics_db==================================================")
                    loadPurchaseMetrics_db(purchase_df, str(queryDate), str(id))
                    lineItems = purchase["attributes"]["lineItems"]
                    if len(lineItems)>0:
                        for lineItem in lineItems:
                            customerId = lineItem['customerId']
                            discountAmount = lineItem['discountAmount']
                            quantity = lineItem['quantity']
                            sku = lineItem['sku']
                            taxAmount = lineItem['taxAmount']
                            total = lineItem['total']
                            unitAmount = lineItem['unitAmount']
                            datalist = [str(id), str(customerId), discountAmount, quantity, str(sku), taxAmount, total, unitAmount, queryDate]
                            lineItem_df = lineItem_df[0:0]
                            length_df = len(lineItem_df)
                            lineItem_df.loc[length_df] = datalist
                            print(lineItem_df)
                            print("====================================loadPurchaselineItemsMetrics_db==================================================")
                            loadPurchaselineItemsMetrics_db(lineItem_df, str(queryDate), str(id), str(customerId), str(sku))
                    print("======================================================================")
            #print("===========================================================")
            #print(response.request.url)
            #print(response.request.headers)
            #print("===========================================================")
            #data = dump.dump_all(response)
            #print(data.decode('utf-8'))
    else:
        print("Error from OAUTH")




