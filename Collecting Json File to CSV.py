
# coding: utf-8

# #### This Notebook is desinged to write each index,json's jspn files to csv
# 
# 
#         
#         

# In[50]:

# Imports
import pprint
import urllib2
import pandas as pd
import json
import time 
import os
import ssl


# In[51]:

# Read in Excel File 
excel_file = pd.read_excel(r'//dc1fs/INTWORK/CSSIP/Data/Health_Data/Backgrond/Machine_Readable_PUF_2015-11-17.xlsx', converters= {
        "Issuer ID": str
    })


# In[52]:

# Convert to pandas dataframe 
index_df = pd.DataFrame(excel_file)


# In[53]:

# get unique urls from the dataframe
# this is stored as a List
url_list = index_df[["Issuer ID","URL Submitted"]].drop_duplicates("URL Submitted").applymap(str)

print url_list 

#Creating a copy of url_list to manipulation
url_list_2 = url_list
#Renaming the colum  "URL Submitted" to URL Unique" before merging 
url_list_2.rename(columns={'URL Submitted' : 'URL Unique'} , inplace=True)
print url_list_2.head()
print "*****"
print index_df.head()
#Addiging a URL Unique column to index_df 
index_df['URL Unique'] = pd.Series(url_list_2['URL Unique'])
print index_df.head()
#Sorting the index_df by "URl Submitted"
index_df = index_df.sort_index(by = 'URL Submitted'  , ascending = 0)
##Re-ordering columns 
cols = index_df.columns.tolist()
print cols
#Placing URL Submitted next to URL Unique 
cols.insert(4, cols.pop(cols.index('Tech POC Email')))
print cols 
#Applying the above changes 
index_df = index_df.reindex(columns= cols)
print index_df.head()
#Exporting teh data to excel 
index_df.to_excel(r'//dc1fs/INTWORK/CSSIP/Data/Health_Data/Backgrond/Before_after_droping_duplicates_merged_d.xlsx')

#Geting back our orinigal url_list 
url_list.rename(columns={ 'URL Unique' :'URL Submitted' } , inplace=True)
# url_list = index_df["URL Submitted"].unique() 
print len(url_list)
url_list = url_list[url_list["URL Submitted"] != "NOT SUBMITTED"]


# In[55]:

# Create values to count the number of Provider.json Files
provider_urls = 0 


# In[58]:

# the number of urls that did not pass the try 
num_timedout = 0 

gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1) 

path_to_files = r"\\dc1fs\INTWORK\CSSIP\Data\Health_Data\Data\Test_Data"


count = 0

# for each unquie url in the list of index urls
for index, row in url_list.iterrows():
    print count 
    if count >= 100: 
        print 'BRAKING!!!'
        break
        
    # Create file to hold output
    target = open(os.path.join(path_to_files, row["Issuer ID"].encode('utf-8') + "_ProviderURLs.csv"), 'w')
    target.write("PROVIDER_URL" + "\n")
    error_txt = ""
    error_count = 0 
    
    # rename for convience 
    this_url = row["URL Submitted"].encode('utf8')
    
    # try to connect to the url and colelct this infomation 
    try:
        # open the index.json
        response = urllib2.urlopen(this_url, context=gcontext) # timeout
        html = response.read()
        #print html
        this_json = json.loads(html)

        # Collect the Provider URLS 
        providers = this_json["provider_urls"]

        #attempt to count this infomation and write it to a file
        try:
            for provider in providers:
                count += 1 
                provider_urls += 1
                target.write(provider.encode('utf-8') + "\n")
        except Exception as e:
            error_txt += "______________________________________________ \\n"
            error_txt += str(e) + "\\n"


    except Exception as e:
        # if unsuccessful abouve, tell me the bad url 
        print e
        print "Error", this_url
        num_timedout += 1 
        print "--------------------------"


    # Close the file 
    target.close()
    if error_count > 0:
        error_log = open(os.path.join(path_to_files, row["Issuer ID"].encode('utf-8') + "_Errors.csv"), 'w')
        error_log.write(error_txt)
        error_log.close()


# In[44]:

print num_timedout
# print the resulting counts             
print provider_urls


# In[57]:

# # open the index.json
# response = urllib2.urlopen('https://www.myparamount.org/machinereadablefiles/cms-data-index.json', context=gcontext)
# html = response.read()
# #print html
# this_json = json.loads(html)

# # Collect the Provider URLS 
# providers = this_json["provider_urls"]
# print providers

target.close()


