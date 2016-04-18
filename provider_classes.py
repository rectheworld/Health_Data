"""
These classes used in the ijson_parse to store values and write them to a csv file 

Primary Author: rectheworld 
"""

import pandas as pd

class Provider():
    """
    Colelcts and holds infomation about a single provider. infomation 
    is stored as variables and pandas dataframes 
    
    the method format_for_csv() compliles this infomation into a final 
    pandas datafram that represents all the infomation for a single provider 
    """
    def __init__(self):
        self.npi = ""
        self.first = ""
        self.last = ""
        self.plans_df = pd.DataFrame(columns= {'plan_id', 'plan_id_type', 'network_tier'})
        self.addresses_df = pd.DataFrame(columns= {'address', 'city', 'state', 'zip', 'phone'})
        self.speciality_df = pd.DataFrame(columns= {'speciality'})
        self.lang_df = pd.DataFrame(columns= {'languages'})
        self.facility_df = pd.DataFrame(columns= {'facility_type'})
        self.provider_type = ""
        self.facility_name = ""
        self.facility_type = ""
        self.last_updated_on = ""
        

    """
    The following methods feeds items into their respectieve dataframes 
    """  

    def plan_handler(self, plans_row_dict):
        self.plans_df = self.plans_df.append([plans_row_dict], ignore_index = True)

    def address_handler(self, address_row_dict):
        self.addresses_df = self.addresses_df.append([address_row_dict], ignore_index = True)
        
    def specialty_handler(self, specialty_row_dict):
        self.speciality_df = self.speciality_df.append([specialty_row_dict], ignore_index = True)
        
    def lang_handler(self, lang_row_dict):
        self.lang_df = self.lang_df.append([lang_row_dict], ignore_index = True)
        
    def facility_handler(self, facility_row_dict):
        self.facility_df = self.facility_df.append([facility_row_dict], ignore_index = True)

    def format_for_csv(self):
        """
        Creates final dataframe from the smaller Dataframes of address, langs, ect
        
        The final dataframe has a many to many relationship 
        
        """  
        self.plans_df['npi'] = self.npi
        self.plans_df['first'] = self.first
        self.plans_df['last'] = self.last
        self.plans_df['provider_type'] = self.provider_type
        self.plans_df['facility_name'] = self.facility_name
        self.plans_df['facility_type'] = self.facility_type
        self.plans_df['last_updated_on'] = self.last_updated_on
        
        #######################
        # Merging with Faclities with plans 
        
        if len(self.facility_df.index) > 1:
            new_df = pd.DataFrame()

            for index, row in self.facility_df.iterrows():
                current_df = self.plans_df.copy(deep = True)

                current_df['facility_type'] = row['facility_type']


                new_df = pd.concat([new_df, current_df], axis=0, join='outer')

            self.plans_df = new_df

        elif len(self.facility_df.index) == 1:
            self.plans_df['facility_type'] = self.facility_df.loc[0,'facility_type']        

        
        ########################
        # Merginging Sepcilites with Plans 
        
        if len(self.speciality_df.index) > 1:
            new_df = pd.DataFrame()

            for index, row in self.speciality_df.iterrows():
                current_df = self.plans_df.copy(deep = True)

                current_df['speciality'] = row['speciality']


                new_df = pd.concat([new_df, current_df], axis=0, join='outer')

            self.plans_df = new_df

        elif len(self.speciality_df.index) == 1:
            self.plans_df['speciality'] = self.speciality_df.loc[0,'speciality']
            
        ##########################
        # Merging Lang with Plans 

        if len(self.lang_df.index) > 1:
            new_df = pd.DataFrame()

            for index, row in self.lang_df.iterrows():
                current_df = self.plans_df.copy(deep = True)

                current_df['languages'] = row['languages']


                new_df = pd.concat([new_df, current_df], axis=0, join='outer')

            self.plans_df = new_df

        elif len(self.lang_df.index) == 1:
            self.plans_df['languages'] = self.lang_df.loc[0,'languages']      
        
        #######################
        # Merginging Addresses with Plans 
        #######################
        
        if len(self.addresses_df.index) > 1:
            new_df = pd.DataFrame()
            
            for index, row in self.addresses_df.iterrows():
                current_df = self.plans_df.copy(deep = True)

                current_df['state'] = row['state']
                current_df['address'] = row['address']
                current_df['city'] = row['city']
                current_df['zip'] = row['zip']
                current_df['phone'] = row['phone']

                
                new_df = pd.concat([new_df, current_df], axis=0, join='outer')
                
            self.plans_df = new_df
            
        else:
            self.plans_df['state'] = self.addresses_df.loc[0,'state']
            self.plans_df['address'] = self.addresses_df.loc[0,'address']
            self.plans_df['city'] = self.addresses_df.loc[0,'city']
            self.plans_df['zip'] = self.addresses_df.loc[0,'zip']
            self.plans_df['phone'] = self.addresses_df.loc[0,'phone']

        
        #print self.plans_df
        #print "FINISHED OBJECT"
        return self.plans_df
        
    
    
        
class Plan_Row():
    """
    Collects and holds infomation for a single plan 
    """
    def __init__(self):
        self.plan_id = ""
        self.plan_id_type = ""
        self.network_tier =""
        
    def plan_feeder(self, prefix, event, value):
        if prefix.endswith('.plan_id'):
            self.plan_id = value
        elif prefix.endswith('.plan_id_type'):
            self.plan_id_type = value
        elif prefix.endswith('.network_tier'):
            self.network_tier = value  
            
            
    def create_dict(self):
        return {'plan_id':self.plan_id, 
                'plan_id_type': self.plan_id_type, 
                'network_tier': self.network_tier}
        
        
        
class Address_Row():
    """
    Collects and holds infomation for a single address 
    """
    def __init__(self):
        self.address = ""
        self.city = ""
        self.state =""
        self.phone = ""
        self.zip_code = ""
        
    def address_feeder(self, prefix, event, value):
        if prefix.endswith('.state'):
            self.state = value
        elif prefix.endswith('.address'):
            self.address = value
        elif prefix.endswith('.city'):
            self.city = value  
        elif prefix.endswith('.zip'):
            self.zip_code = value  
        elif prefix.endswith('.phone'):
            self.phone = value  
            
    def create_dict(self):
        return {'state':self.state, 
                'address': self.address, 
                'city': self.city,
               'zip': self.zip_code,
               'phone': self.phone}
        

class Facility_Type_Row():
    """
    Collects and holds infomation for a facility type 
    """
    def __init__(self):
        self.facility_type = ""
        
        
    def facility_feeder(self, prefix, event, value):
        if prefix.endswith('.item'):
            self.facility_type = value

    def create_dict(self):
        return {'facility_type': self.facility_type}
    

class Specialty_Row():
    """
    Collects and holds infomation for a Specialty Row 
    """
    def __init__(self):
        self.specialty = ""
        
    def specialty_feeder(self, prefix, event, value):
        if prefix.endswith('.item'):
            self.specialty = value

    def create_dict(self):
        return {'speciality': self.specialty}
        
        
class Lang_Row():
    """
    Collects and holds infomation for a language 
    """
    def __init__(self):
        self.lang= ""
        
    def lang_feeder(self, prefix, event, value):
        if prefix.endswith('.item'): 
            self.lang = value

    def create_dict(self):
        return {'languages': self.lang}
        