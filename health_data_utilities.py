"""
Functions saved here are used either in the exploarion of Provider.json data
or the harvesting of that data and wrting to csvs 

Actral Parsing Functions are saved in parsers.py for conviencinence. 



get_provider_urls
    - Returns a list of provider urls under and index url 
    
get_json
    - extracts json from a url

get_json_via_ijson
    - creates an ijson parser object from a url 

get_unquie_keys
    - Idenifies if the first 15 objects in a json object have the same keys 


Authors: Mian2512 & Rectheworld 
Last Updated: 3/24/2016

"""

# Putting these here for now 
import pandas as pd
import urllib, json
import urllib2
import re
from requests import get
import ssl
from pandas.io.json import json_normalize
from operator import itemgetter
import pprint


# used on urllib requests 
#gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
gcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv3)

def get_provider_urls(index_url):
    """
    Retrived the Provider urls under and index url 

    Parameters
    ----------
    index_url - the url of a health data provider's index.json

    Returns
    -------
    A list of provider urls 

    """
    #Open the url and load the json file to retrive provider links we need to follow 
    response = urllib.urlopen(index_url , context=gcontext)
    data = json.loads(response.read())

    #Storing all the provider links in "links" variable
    links = data['provider_urls']

    #Checking "links" to see what we have
    # print 'all links: ' + str(links)
    # print 'link 1: ' + links[0]
    
    return links 




def get_json(url):
    """
    Reterivesd sdasdasd from url 

    Parameters
    ------------
    url - a json url 

    Returns
    ------
    the contents of that url 

    """

    try:
        response = urllib2.urlopen(url , context = ssl.SSLContext(ssl.PROTOCOL_TLSv1))  
    except: 
        response = urllib2.urlopen(url , context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)) 

    # print 'response: ' + str(response)  

    return response


def get_json_via_ijson(url):
    """
    This function creates a parser object from a json url 
    
    Paratmeters
    -----------
    url - an url ending in .json
    
    Returns 
    -------
    An ijson parsing object 
    """
    from ijson import parse
    
    # load the json
    f = urllib2.urlopen(url, context = gcontext)
    # PArse the json
    parser = parse(f)
    return parser  








def get_unquie_keys(parser, number_of_objects = 10):
    """
    Tests to see if the first x number of json onjects in 
    a file have the same shema
    
    Parameters
    ----------
    parser - an ijson parser object
    number_of_objects - the number of json objects this we should test.
    this numer will represent the first x objects.
    
    Returns
    -------
    True - if they all all have the same shema
    
    if the result is false, it will return a list of sets of the diffent keys 
    
    ###############
    USE WITH IJSON PARSER
    ###############
    """
    # list of unqiue schemas. 
    list_of_keys = []
  

    for i in range(0,number_of_objects):
        # set of keys for this json instance 
        this_keys = set()
        
        # for Each  of these ie,  item.plans.item.network_tier string PREFERRED
        for prefix, event, value in parser:
            
            # if the event is map_key it we are looking at a new parent key
            # or item key 
            # Check to see if we are at a new key and that this key is not 
            # already in the set of keys 
            if event == "map_key" and value not in this_keys:
                # Add this key to the set 
                this_keys.add(value)
                #print value 
                
            if (prefix, event, value) == ('item', 'end_map', None):
                #print "BREAKING!!!!!!!!!!!!!!!!!!!!!!!"
                
                # check to see if this is a shemema That has already been 
                # recorded in list_of_Keys 
                if len(list_of_keys) == 0:
                    list_of_keys.append(this_keys)
                else:
                    matches = 0
                    for key_set in list_of_keys:
                        if key_set == this_keys:
                            matches += 1 
                            
                    if matches == 0:
                        list_of_keys.append(this_keys)
                
                # Increment the jsonobject itterator            
                i += 1 
                break
    #----------------End Of Object itterator-------------------
    
    if len(list_of_keys) == 1:
        return True 
    else:
        print "Therse are diffenrt Shemas in this group of objects: "
        pprint.pprint(list_of_keys)



def get_first_key(my_response): 

        '''
        This function returns the first key of a JSon Object that will be passed into regular expression parser to split chunks 

        ********
        Parameter:
        my_response = JSon object we want to get the first key from 

        ********
        Return 
        first_key
        '''
        #Declaring variable 
        first_key = ""
        chunk = my_response.read(4*8) #Divide the JSon object into chunks separated by npi
        #Using regular expression to search for the first key in chunk
        first_key =  re.search("^[\W_]+([\w_]+)" , chunk ).group(1)
        return first_key
