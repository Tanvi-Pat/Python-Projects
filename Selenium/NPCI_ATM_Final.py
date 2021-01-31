#!/usr/bin/env python
# coding: utf-8

# In[26]:


from selenium import webdriver
from scrapy import Selector
import requests
import pandas as pd
import csv
import threading
import multiprocessing
import subprocess


# In[17]:


state_pincode_path = "T:\\state_pincode.csv"
pincode_df = pd.DataFrame(columns = ['Bank','Address','Landmark','Postal PIN','District/City','Metro/Non-Metro'])
dest_file= "T:\\Scrapped_ATMs.csv"
dest_file1= "T:\\Scrapped_ATMs1.csv"
source_file = "T:\\Goa_pin.csv"


# In[23]:


chromedriver_location = "C:\\Users\\tanvip\\Downloads\\chromedriver"
driver = webdriver.Chrome(chromedriver_location)
url = 'https://www.npci.org.in/atm/locator/demo?'
driver.get(url)


# In[24]:


get_ipython().run_cell_magic('time', '', "\nwith open(source_file,'r') as file:\n    reader = csv.reader(file)\n    for row in reader:\n        try:\n            state_pincode(row[0],row[1])\n        except Exception as e: \n            driver = webdriver.Chrome(chromedriver_location)\n            url = 'https://www.npci.org.in/atm/locator/demo?'\n            driver.get(url)")


# In[ ]:


get_ipython().run_cell_magic('time', '', "threads=[]\n\nwith open(source_file,'r') as file:\n    reader = csv.reader(file)\n    for row in reader:\n        t=threading.Thread(target=state_pincode,args=(row[0],row[1]))\n        t.start()\n        threads.append(t)\n    print(len(threads))\n    for t in threads:\n        t.join() ")


# In[5]:


def state_pincode(state,pincode):
    print('entered state_pincode func',state,pincode)
    driver.get(url)
    input_val = driver.find_element_by_id('edit-state')
    input_val.send_keys(state)
    input_val = driver.find_element_by_id('edit-pincode')
    input_val.send_keys(pincode)
    input_val.submit()
    url1= driver.current_url
    scrap_atm()


# In[6]:


def scrap_atm():
    while(True):
        try:
            response = requests.get(driver.current_url)
            sel_base = Selector(text = response.content, type = "html")
            text = sel_base.css('div.atm_table_container table tbody tr td::text').getall()
            write(text,dest_file1) 
            python_button = driver.find_elements_by_xpath('//div[@class="atm_table_container"]/div[@class="item-list"]/ul/li[@class="pager-next"]')[0]
            python_button.click()
        except Exception as e: 
            #print(e)
            break;


# In[7]:


def write(args,dest_file):
    with open(dest_file,'a',newline = '', encoding='utf-8') as file:
        writer = csv.writer(file)
        i=0
        while(i<=(len(args)-6)):
            args[i]=str(args[i]).upper()
            args[i+4]=str(args[i+4]).upper()
            city,district= args[i+4].split(',')
            args[i+1] = args[i+1].translate(str.maketrans('', '', '"'))
            writer.writerow([args[i],args[i+1],args[i+2],args[i+3],city,district.strip(),args[i+5]])
            i+=6
    file.close()

#pin_df = pin_df.append({'Bank': args[i],'Address': args[i+1], 'Landmark': args[i+2], 'Postal PIN': args[i+3], 'District/City': args[i+4], 'Metro/Non-Metro': args[i+5]},ignore_index = True)    


# In[25]:


filepath = str('"' + 'T:\\csv_to_postgre_table.bat' + '"')
print(filepath)
bat = subprocess.Popen(filepath,stdout = subprocess.PIPE,shell = True, stderr = subprocess.STDOUT)
bat.communicate()


# In[48]:


import string
ar = 'GOA, GOA'
city, district=ar.split(',')
city
district.strip()


# In[ ]:




