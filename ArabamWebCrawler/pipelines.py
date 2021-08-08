import os
import pandas as pd
import re
import csv
from csv import reader
from sqlalchemy import create_engine
import psycopg2
import numpy as np
import datetime
from ArabamWebCrawler.items import ArabamWebCrawlerItems

#Functions
def trim_all_columns(df):
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


#Lists
cols =['ad_number', 'title', 'price', 'currency', 'province', 'district', 'ad_date', 'brand', 'serial', 'model', 'year', 'fuel_type', 'gear_type', 'engine_capacity', 'motor_power', 'kilometers', 'from_w']
months=["Jan","Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
aylar =["Ocak","Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]
list = []


class ArabamWebCrawlerPipeline():
    #Reading Data
    try:
        with open('/home/gokhan/data/arabam_raw.csv', 'r') as read_obj:
            csv_reader = reader(read_obj)
            #Cleaning Data
            for row in csv_reader:
                item = ArabamWebCrawlerItems()
                for col in row:            
                    x = col.replace("'","").replace('"','').replace("[","").replace("]","").replace(":,",":").replace("  ", "").replace(",", "").replace("km", "").replace("HP", "").replace("hp", "").replace("'ye kadar", "").replace("' ye kadar", "").replace("e kadar", "").replace(" e kadar", "").replace("'e kadar", "").replace("' e kadar", "").replace("cm3", "").replace("cc", "").strip() 
                    y = col.replace(":,",":").replace("  ", "").replace(",", "").replace("km", "").replace("TL", "").replace("USD", "").strip()
                    z = col.replace(":,",":").replace("  ", "").replace(",", "").replace("km", "").replace("/", ":").strip() 
                    t = col.replace(":,",":").replace("  ", "").replace(",", "").replace("km", "").replace("HP", "").replace("hp", "").replace("'ye kadar", "").replace("' e kadar", "").replace("cm3", "").replace("cc", "").replace(" - ", ":").strip() 
                    for i in range (len(months)): 
                        col = col.replace(aylar[i], months[i]).replace(",", "").strip() 
                    time = col
                    dim = len(x.split(':'))               
                    if(dim < 2):
                        continue
                    heading = x.split(':')[0]
                    value_x = x.split(':')[1]
                    value_y = y.split(':')[1]
                    value_z1 = z.split(':')[1]
                    value_z2 = z.split(':')[-2]
                    value_t = t.split(':')[1]
                    value_time = time.split(':')[1]               
                    if heading == 'title':
                        item.title = value_x                
                    if heading == 'price':              
                        if  " TL" in value_x:
                            item.currency = "TL"
                        if  " USD" in value_x:
                            item.currency = "USD" 
                        item.price = value_y    
                    if heading == 'address':
                        item.province = value_z1
                        item.district = value_z2
                    if heading == 'İlan No':
                        item.ad_number = value_x   
                    if heading == 'İlan Tarihi':
                        item.ad_date = value_x
                        item.ad_date = value_time
                    if heading == 'Marka':
                        item.brand = value_x   
                    if heading == 'Seri':
                        item.serial = value_x   
                    if heading == 'Model':
                        item.model = value_x   
                    if heading == 'Yıl':
                        item.year = value_x   
                    if heading == 'Yakıt Tipi':
                        item.fuel_type = value_x  
                    if heading == 'Vites Tipi':
                        item.gear_type = value_x     
                    if heading == 'Motor Hacmi':
                        item.engine_capacity = value_x     
                    if heading == 'Motor Gücü':
                        item.motor_power = value_t    
                    if heading == 'Kilometre':
                        item.kilometers = value_x     
                    if heading == 'Kimden':
                        item.from_w = value_x         
                if item.ad_number:
                    list.append(item)
    except IOError:
        raise

    #Creating dataframe and Changing dtypes           
    df = pd.DataFrame([[getattr(i,j) for j in cols] for i in list], columns = cols)
    df = trim_all_columns(df)
    df = df.replace("","-9")
    df["ad_number"] = df['ad_number'].astype('int')
    df["motor_power"] = df['motor_power'].astype('int')
    df["ad_date"] = pd.to_datetime(df["ad_date"]).dt.strftime('%Y-%m-%d')
    df["ad_date"] = df['ad_date'].astype('datetime64')
    df = df.convert_dtypes()
    
    #Writing dataframe to postgresql database
    engine = create_engine('postgresql://gokhan:Cakal2020!@10.34.39.10:5432/tcd')
    df.to_sql('arabam', engine,if_exists='append', index=False, chunksize=10000)
    
    #Deleting raw data
    try:
        pass
        os.remove("/home/gokhan/data/arabam_raw.csv")
    except IOError:
        raise