#!/usr/bin/env python
# coding: utf-8

# In[79]:


from selenium import webdriver
import pandas as pd
import time
import sqlalchemy
import s3fs
import os
import glob


# In[80]:

#aws parameters
aws_access_key = 'yours3accesskey'
aws_secret_key = 'yous3secretkey'
aws_bucket = 'my_bucket'
aws_path = 'my_path'
aws_file_name = 'file_name.csv'

#server parameters
username = 'myuser'

#selenium parameters
driver_exe = 'path_to_driver_exe.exe'

# Check if the file you want to download already exists in the destination path. If exists, delete it
fileList = glob.glob(fr'C:\Users\{username}\Downloads\*partial_file_name*')

for filePath in fileList:
    try:
        os.remove(filePath)
        print(f'File deleted: {filePath}')
    except:
        print(f'Error while trying to delete file: {filePath}')


# In[81]:


#automation to download file from web. In the example, we download an Excel file which contains historical data from currency exchange between dollars and argentinian pesos
chrome_browser = webdriver.Chrome(f'{driver_exe}')

chrome_browser.maximize_window()
chrome_browser.get('http://www.rava.com/empresas/perfil.php?e=DOLAR%20CCL')

time.sleep(10)

download_excel = chrome_browser.find_element_by_partial_link_text('Bajar en formato Excel')
download_excel.click()

time.sleep(10)
chrome_browser.close()


# In[82]:


#Read downloaded file to create a pandas data frame
usd_ccl_csv = glob.glob('PATH\*partial_file_name*')
file_name = usd_ccl_csv[0]
usd_ccl_df = pd.read_csv(file_name, sep = ',')


# In[83]:


#establish connection to s3 bucket and write downloaded file to it
s3 = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key,anon=False)
filename = f'{aws_bucket}/{aws_path}/{aws_file_name}'
with s3.open(filename, 'w', newline='') as f:
    usd_ccl_df.to_csv(f, index=False, header=False, sep= '|', quotechar='%')


# In[84]:


#Establish connection to redshift database
con = sqlalchemy.create_engine('postgresql://USER:PASSWORD@DNS:PORT/DATABASE')

#Get file from s3 and write it's data to a redshift table
con.execute("""
    DELETE TABLE_NAME;
    COPY TABLE_NAME
    from 's3://%s'
    credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
    delimiter '|' 
    removequotes
    emptyasnull
    blanksasnull
    ;""" % (filename, aws_access_key, aws_secret_key))

