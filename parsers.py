"""

Parsers used to collect infomation from a provider url and save the results to a csv 


prefered parser is regular_expression_parse

regular_expression_parse
	- Uses reglar expression to parse a provider json url and write it to a csv 

ijson_parse
	- uses ijson to parse a provider url and write it to a csv 

write_to_csv
    - Helper funtion to the regular_expression_parse function 
        - refomates a dataframe for a csv format 



Authors: Mian2512 & Rectheworld 
Last Updated: 3/24/2016

"""

# Imports 
import pandas as pd 
import pprint
import urllib2
import json
import jsonschema
#from ijson import parse
import os 
import random 

from health_data_utilities import * 

from provider_classes import *

def regular_expression_parse_key(url, provider_id, destination_path):
    """
    Uses regualr expression to parse provider objects out of a json data 
    format and then write them to a csv. 

    Parameters
    ----------
    url - the provider url
    provider_url : if False, url is an index url, otherwise itis a provider url  

    provider_id - A string representing the numerical ID of a provider(?). Used
        When nameing files 

    Returns
    --------
    VOID 

        - writes a csv named "Provider_info.csv" to the current working directy 

    """

    # Request the information from that URL 
    response = get_json(url)

    key = get_first_key(response)


    # initlizie these vlaues  # PLEASE COMMENT AS TO WHAT THESE RESEPESENT 
    num = 0 # 
    start = '' # 

    # For checking if a url was succesfully parsed or bad 
    error = '' # 
    bad_url = False 
    reached_end = False

    # Flow Control 
    running = True
    chunk_count = 0 # used to keep track of how many chunks we have taken 
    chunk_set = 0 # used to keep track of how many set of chucnks we have sent to files 

    npi_count = 0 
    npi_list = []

    final_df = pd.DataFrame(columns = ['accepting', 
        'address', 
        'address2', 
        'facility_name', 
        'facility_type', 
        'languages', 
        'last_updated_on', 
        'npi',  
        'speciality', 
        'type',
        'first',
        'last'])

    while running:
    #4) Divide the JSon object in #3 above into chunks separated by "npi"
    # PLease DON'T delete this comment for now,    chunk = re.sub('[\n\t\r\f]+','',response.read(64*1024)).split('"npi"')


        # Counter and Progrees indicator to write every 10 chuncks to a file 
        chunk_count += 1
        

        #4) Divide the JSon object in #3 above into chunks separated by "npi"
        # PLease DON'T delete this comment for now,    chunk = re.sub('[\n\t\r\f]+','',response.read(64*1024)).split('"npi"')
        chunk = response.read(64*2024).strip().split('"%s"' % key )  #Divide the JSon object into chunks separated by npi
        
        #5)Defines the starting point of the chunk  
        if num == 0: 
            #print chunk[0]
            del chunk[0]
            num = 1

        chunk[0] = start+chunk[0]
        start = chunk[-1]
        #print len(chunk)
        
        #6) Loads all JSON objects in chunk 
        #When length of chunk is >1 we need to loop through chunk
        if  len(chunk) !=1 :     
            # C is an individual provider object, so for each object in the chunk
            for c in chunk[:-1]:

                    # Please DON'T DELETE THIS COMMENT, resp = json.loads('{"npi"'+re.sub(',{\s+$','',c))  

                # Attenpt to clean the data, will throw error is inapprpate parser is used 
                
                # try:
                #     # Instade c object that has neen clena of whitespace and the leading ,{ , 
                #     # Substatute ,{$ with a balnk space
                #     # concatanate npi to the front of the resulting string 
                #     resp_dict = json.loads('{"%s"' % key+re.sub(',{$','',c.strip().lstrip(',{'))) 
                # except:
                #     try: # alternate Syntax 1 
                #         c =  re.sub('{$','', c.strip())
                #         c = re.sub(',$','',c.strip())
                #         resp_dict = json.loads('{"%s"' % key+ c) 

                #     except Exception as e:
                #         print e 
                #         break

                try: # alternate Syntax 1 
                    c =  re.sub('{$','', c.strip())
                    c = re.sub(',$','',c.strip())
                    resp_dict = json.loads('{"%s"' % key+ c) 

                except Exception as e:
                    error =  e 
                    print error
                    break

                npi_list.append(resp_dict['npi']) 
                    # print "LENGTH OF RESP DICT" , len(resp_dict)

                

                # This allows us to work with large json files without running out of memory 
                
                temp_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in resp_dict.iteritems() ]))

                this_object_df = write_to_csv(temp_df, resp_dict)

                final_df = final_df.append(this_object_df, ignore_index = False)

                    #Export this to csv 
                #There should be errors but if there are just print them for now    



            # Once we finished to go through the chunk we exit the loop     
            if not chunk:  
                exit()
                
        #Parsing the last chunk as a stand alone, if there is only one object in the chunk... 
        elif len(chunk) ==1 :
            #print chunk 

            # # Attenpt to clean the data, will throw error is inapprpate parser is used 
            # try:
            #     # Instade c object that has neen clena of whitespace and the leading ,{ , 
            #     # Substatute ,{$ with a balnk space
            #     # concatanate npi to the front of the resulting string 
            #     resp_dict = json.loads('{"%s"' %key +re.sub(',{$','',c.strip().lstrip(',{'))) 
            # except:

            for c in chunk[:-1]:

                try: # alternate Syntax 1 
                    c =  re.sub('{$','', c.strip())
                    c = re.sub(',$','',c.strip())
                    resp_dict = json.loads('{"%s"' %key + c) 
                    print cake 
                    
                except Exception as e:
                    error =  e 
                    print error
                    break

                temp_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in resp_dict.iteritems() ]))

                this_object_df = write_to_csv(temp_df, resp_dict)

                final_df = final_df.append(this_object_df, ignore_index = False)

                # print "****"
                # print resp_dict['npi']
                # print resp_dict

            # Because we have reached the end of this urls, set reach end equal to true 
            reached_end = True 
            # We have reached the end of the provider_url, thus break this loop 
            running = False
            
        
        else :
            exit()

        # if we have processed 50 chunks, send the final dataframato to a csv
        if chunk_count == 50:
            
            # we have completed a set, increment the chunk set count
            chunk_set += 1

            # add in a col to show what provider this data came from 
            final_df['provider_url'] = url

            # get number of npis from this group of data
            npi_count += len(list(final_df['npi'].unique()))

            result_path = destination_path + "//provider_info_" + provider_id + "_FILE_" + str(chunk_set) + ".csv"

            final_df.to_csv(result_path, encodeing = "utf-8", index = False)

            # reinililize the final_df
            final_df = None
            final_df = pd.DataFrame(columns = ['accepting', 
                'address', 
                'address2', 
                'facility_name', 
                'facility_type', 
                'languages', 
                'last_updated_on', 
                'npi',  
                'speciality', 
                'type',
                'first',
                'last'])

            # reset chunk_count
            chunk_count = 0 


        # # used for TEsting Each Chunch set will have 10 chunks in it, when we reaht that number stop 
        # if chunk_set == 1: running = False 
    # ------------------------- End of While Loop -------------------------------------    

    # Write the remaining chunks to a csv 
    chunk_set += 1

    # add in a col to show what provider this data came from 
    final_df['provider_url'] = url

    # get number of npis from this group of data 
    npi_count += len(list(final_df['npi'].unique()))

    # get length of npi set
    npi_parsed = len(set(npi_list))

    result_path = destination_path + "//provider_info_" + provider_id + "_FILE_" + str(chunk_set) + ".csv"

    final_df.to_csv(result_path, encodeing = "utf-8", index = False)

    # Was this url succesfully parsed? a npi count of zzero would indicate not 
    if npi_parsed == 0 or npi_count == 0 or error != '':
        bad_url = True 
    else: 
        bad_url = False  

    result_dict = {'npi_parsed': npi_parsed, 'npi_count': npi_count, "bad_url": bad_url, "error": error, "url_completed": reached_end}

    return result_dict


def ijson_parse(provider_url):
    """
    Uses Ijson to parse a provider url and write the data to a csv file.
    
    Json objects are read line by line and passed to classes to manged and merge
    dataframe objects.
    
    the final data frame is written to a csv 
    
    
    Parameters
    ----------
    provider_url - the json url of a provider 
    
    Returns
    --------
    VOID - writes results to CSV 
    
    
    Example 
    -------
    If an Provider has two addresses and three plans the result will 
    look like 

    npi         adddress        plans 
    ---         --------        ------
    12345       address_1       plan_1
    12345       address_1       plan_2
    12345       address_1       plan_3
    12345       address_2       plan_1
    12345       address_2       plan_2
    12345       address_2       plan_3


    It is possible for a provider to have multiple address, plans, langs,
    facility types and specialities. These feilds will be replcated in the 
    final csv so that each feild is connected to all the values of other feilds    
    

    """



    # for testing 
    count = 0
    
    # set up to collect the json from the url via parser object 
    parser = get_json_via_ijson(provider_url)

    # intilize a dataframe that will hold
    # the final result 
    master_df = pd.DataFrame() 
    
    # for Efficency, if it is known we are in an subobject that is 
    # known to have multiple values, we will funnel that 
    # prefix, event, value tuple to the class for that sub object to be saved
    
    # the funneling will do controlled with the variable STATE
    # STATE can take the values of,
    # SINGLE - a normal key : value pair
    # ADDRESS - representing we are in an address object, ect 
    STATE = 'SINGLE'
    
    # The parser object can be iterated by stepping through tuples containing 
    # an prefix, event, and value 
    # Prefix
    #  - the type of item we are looking at. The top level of an json object is call an 'item'
    # and the items under that object are designated with dot notation for example an npi of a 
    # provider object is designated as 'item.npi'
    # Event 
    # - Used to identify the kind of object the value is. for example a list is a 'map_key' but the 
    # in a list may be a 'string'
    # Value 
    # - the vlaue of the event. if event is a map, the vlaue is None. if the event is an item, the
    # value is equal to that value 
    for prefix, event, value in parser:
        
        # Funnel ... 
        if STATE == 'ADDRESS':
            this_address.address_feeder(prefix, event, value)

        elif STATE == 'PLAN':
            this_plan.plan_feeder(prefix, event, value)

        elif STATE == 'SPECIALITY':
            this_specality.specialty_feeder(prefix, event, value)

        elif STATE == 'LANG':
            this_lang.lang_feeder(prefix, event, value)

        elif STATE == 'FACILITY':
            this_facility.facility_feeder(prefix, event, value)


        #################################
        # Create New Object 
        ###################################
        # the tuple 'item', 'start_map', None 
        if (prefix, event, value) == ('item', 'start_map', None):
            this_object = Provider()

        #################
        # Speciality 
        #################  

        elif (prefix, event, value) == ('item.speciality', 'start_array', None):
            this_specality = Specialty_Row()
            STATE = 'SPECIALITY'

            # now for the each progressive pience of infomation should feed this object 
            # until we reach the end of this object 

        elif (prefix, event, value) == ('item.speciality', 'end_array', None):

            this_object.specialty_handler(this_specality.create_dict())

            this_specality = None
            STATE = 'SINGLE'    

        #################
        # Lang
        #################  
        elif (prefix, event, value) == ('item.languages', 'start_array', None):
            this_lang = Lang_Row()
            STATE = 'LANG'
            # now for the each progressive pience of infomation should feed this object 
            # until we reach the end of this object 

        elif (prefix, event, value) == ('item.languages', 'end_array', None):

            this_object.lang_handler(this_lang.create_dict())

            this_lang = None
            STATE = 'SINGLE'        


        #################
        # Facilities 
        #################  

        elif (prefix, event, value) == ('item.facility_type', 'start_array', None):
            this_facility = Facility_Type_Row()
            STATE = 'FACILITY'


            # now for the each progressive pience of infomation should feed this object 
            # until we reach the end of this object 

        elif (prefix, event, value) == ('item.facility_type', 'end_array', None):

            this_object.facility_handler(this_facility.create_dict())

            this_lang = None
            STATE = 'SINGLE'        


        #################
        # Addresses
        #################


        elif (prefix, event, value) == ('item.addresses.item', 'start_map', None):
            this_address = Address_Row()
            STATE = 'ADDRESS'

            # now for the each progressive pience of infomation should feed this object 
            # until we reach the end of this object 

        elif (prefix, event, value) == ('item.addresses.item', 'end_map', None):

            this_object.address_handler(this_address.create_dict())

            this_address = None
            STATE = 'SINGLE'


        ##########################
        # Plans 
        ##########################
        elif (prefix, event, value) == ('item.plans.item', 'start_map', None):
            this_plan = Plan_Row()
            STATE = 'PLAN'

            # now for the each progressive pience of infomation should feed this object 
            # until we reach the end of this object 


        elif (prefix, event, value) == ('item.plans.item', 'end_map', None):
            this_object.plan_handler(this_plan.create_dict())
            this_plan = None 
            STATE = 'SINGLE'
            #pprint.pprint(this_object.plans_df)


        elif prefix.endswith('.npi'):
            this_object.npi = value 
        elif prefix.endswith('.last_updated_on'):
            this_object.last_updated_on = value 
        elif prefix.endswith('.first'):
            this_object.first = value 
        elif prefix.endswith('.last'):
            this_object.last = value 
        elif prefix.endswith('.facility_name'):
            this_object.facility_name = value 
        elif prefix.endswith('.type'):
            this_object.provider_type = value

        #############################
        # The End of a Provider Object
        #############################
        elif (prefix, event, value) == ('item', 'end_map', None):
            try:
                master_df = pd.concat([master_df, this_object.format_for_csv()], axis = 0, join = 'outer')
            except Exception as e:
                print e

            # increase itterater for testing 
            count += 1
            
        # for testing 
        if count == 10:
            break 

    # Write resutls to File 
    master_df.to_csv('ijson_parsed_file.csv')



#######################################
# Helper Functions
#######################################

def write_to_csv(temp_df, object_dict):
    """"

    Transfroms raw infomation about a provider into an DataFrame.

    The Dataframe produced has a many to many relationship.

    The proccess of creating the final dataframe invloves createing 
    a dataframe out of the keys where is it possible to mave mutiple 
    values, for example language can be a list of ENGLISH, GERMAN, ECT 

    the Dataframes are then merged together with the Plans dataframe 
    one at a time until there is a many to many relationship between the 
    different values of the feilds. The Plans dataframe is used as the saffolding 
    for the final dataframe becaue it is assumed that there are more vlaues in plans 
    than any other feild. 



    Parameters
    ----------
    temp_df - a dataframe produced by pasing a python dictononary from the
        python loads object to the command,
         pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in object_dict.iteritems() ]))

    object dict - the dictonary creaded by the python loads object on a provider json
        object 


    Returns 
    -------

    A pandas Dataframe with tha many to many to many relationship. 


    Example
    ------------------
    If an Provider has two addresses and three plans the result will 
    look like 

    npi         adddress        plans 
    ---         --------        ------
    12345       address_1       plan_1
    12345       address_1       plan_2
    12345       address_1       plan_3
    12345       address_2       plan_1
    12345       address_2       plan_2
    12345       address_2       plan_3


    It is possible for a provider to have multiple address, plans, langs,
    facility types and specialities. These feilds will be replcated in the 
    final csv so that each feild is connected to all the values of other feilds


    Called in json_to_csv()

    """

    ##############################
    # Create DataFrames 
    ##############################
    # Create Address DF 
    this_address_df = pd.DataFrame()
    for index,row in temp_df['addresses'].iteritems():
        temp_address_df = pd.DataFrame([temp_df['addresses'][index]]).dropna(how = 'all')
        this_address_df = pd.concat([this_address_df, temp_address_df], axis = 0, join = 'outer')

    # if address2 is spelled address_2, rename it 
    if 'address_2' in list(this_address_df.columns):
        this_address_df.rename(columns={'address_2': 'address2'}, inplace=True)

    # Create Facility type DF 
    this_faclity_df = pd.DataFrame(columns = ['facility_type'])
    if  object_dict.has_key('facility_type'):
        for item in object_dict['facility_type']:

        	# Force the item being written to be uppercase 
            this_faclity_df.loc[len(this_faclity_df)] = item.encode('utf-8').strip().upper()

    # Create Specialty  DF 
    this_specilty_df = pd.DataFrame(columns = ['speciality'])
    if  object_dict.has_key('speciality'):
        for item in object_dict['speciality']:

        	# Force the item being written to be uppercase 
            this_specilty_df.loc[len(this_specilty_df)] = item.encode('utf-8').strip().upper()

    # Create Langduride df 
    this_lang_df = pd.DataFrame(columns = ['languages'])
    if  object_dict.has_key('languages'):
        for item in object_dict['languages']:

        	# Force the item being written to be uppercase 
            this_lang_df.loc[len(this_lang_df)] = item.encode('utf-8').strip().upper()

    # Create Plans DF
    this_plans_df = pd.DataFrame()
    for index,row in temp_df['plans'].iteritems():
        temp_plans_df = pd.DataFrame([temp_df['plans'][index]]).dropna(how = 'all')
        this_plans_df = pd.concat([this_plans_df, temp_plans_df], axis = 0, join = 'outer')

    ##############################
    # Merging
    ##############################

    # Merge Falilities with Plans 
    this_faclity_df = this_faclity_df.drop_duplicates('facility_type')
    if len(this_faclity_df.index) > 1:
        new_df = pd.DataFrame()
        
        for index, row in this_faclity_df.iterrows():
            current_df = this_plans_df.copy(deep = True)

            current_df['facility_type'] = row['facility_type']

            
            new_df = pd.concat([new_df, current_df], axis=0, join='outer')
            
        this_plans_df = new_df
        
    elif len(this_faclity_df.index) == 1:
        this_plans_df['facility_type'] = this_faclity_df.loc[0,'facility_type']


    # Merge Specialties with Plans 
    # select only uniqie vlaues
    this_specilty_df = this_specilty_df.drop_duplicates('speciality')
    if len(this_specilty_df.index) > 1:
        new_df = pd.DataFrame()
        
        for index, row in this_specilty_df.iterrows():
            current_df = this_plans_df.copy(deep = True)

            current_df['speciality'] = row['speciality']

            
            new_df = pd.concat([new_df, current_df], axis=0, join='outer')
            
        this_plans_df = new_df
        
    elif len(this_specilty_df.index) == 1:
        this_plans_df['speciality'] = this_specilty_df.loc[0,'speciality']

    # Merge lang with Plans 
    # select only uniqie vlaues
    this_lang_df = this_lang_df.drop_duplicates("languages") 
    if len(this_lang_df.index) > 1:
        new_df = pd.DataFrame()
        
        for index, row in this_lang_df.iterrows():
            current_df = this_plans_df.copy(deep = True)

            current_df['languages'] = row['languages']

            
            new_df = pd.concat([new_df, current_df], axis=0, join='outer')
            
        this_plans_df = new_df
        
    elif len(this_lang_df.index) == 1:
        this_plans_df['languages'] = this_lang_df.loc[0,'languages']


    # Merge the addresses and plans 
    if len(this_address_df.index) > 1:
        new_df = pd.DataFrame()

        for index, row in this_address_df.iterrows():


            current_df = this_plans_df.copy(deep = True)

            current_df['state'] = row['state']
            current_df['address'] = row['address']
            
            current_df['address2'] = row['address2'] if "address2" in list(this_address_df.columns) else None

            current_df['city'] = row['city']
            current_df['zip'] = row['zip']
            current_df['phone'] = row['phone']

            new_df = pd.concat([new_df, current_df], axis=0, join='outer')


        this_plans_df = new_df
        
    else:
        if not this_address_df.empty:
            this_plans_df['state'] = this_address_df.loc[0,'state'] 
            this_plans_df['address'] = this_address_df.loc[0,'address'] 

            this_plans_df['address2'] = this_address_df.loc[0,'address2'] if "address2" in list(this_address_df.columns) else None
            this_plans_df['city'] = this_address_df.loc[0,'city'] 
            this_plans_df['zip'] = this_address_df.loc[0,'zip'] 
            this_plans_df['phone'] = this_address_df.loc[0,'phone']


    # Fill in the singlar values 
    this_plans_df['npi'] = object_dict['npi']
    this_plans_df['first'] = object_dict['name']['first'] if object_dict.has_key('name') else ""
    this_plans_df['last'] = object_dict['name']['last'] if object_dict.has_key('name')else ""
    this_plans_df['provider_type'] = object_dict['type'] 
    this_plans_df['last_updated_on'] = object_dict['last_updated_on'] if object_dict.has_key('last_updated_on')else ""
    this_plans_df['facility_name'] = object_dict['facility_name'] if object_dict.has_key('facility_name') else ""
    this_plans_df['type'] = object_dict['type'] if object_dict.has_key('type') else ""


    # Add a hash value to serve as a unique identifyer 
    this_plans_df['unique_identifier'] = random.getrandbits(24)


    # Return this_plan as the final DataFrame 
    return(this_plans_df)