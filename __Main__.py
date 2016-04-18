import pandas as pd 
import pprint
import urllib2
import json
import jsonschema
#from ijson import parse
import argparse
from glob import * 
from twilio.rest import TwilioRestClient
accountSID = 'ACc81a03cdf9c3567c17d09cd1944497c0'
authToken = '36e8239d0a2cec96b9aea1c14f938bec'
twiloClient = TwilioRestClient(accountSID,authToken)
mytwilioNumber = '+18179853782'
myCellPhone = '+'
import os

import linecache
import sys 

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    result = 'EXCEPTION IN ({} LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    return result



from health_data_utilities import * 
from parsers import regular_expression_parse_key
from parsers import write_to_csv 

arg_parser = argparse.ArgumentParser(description="Collectets provider urls from csv files, retrived the data from json, and bla blabla ")

arg_parser.add_argument(dest="url_directory", action="store", help="The directory path to the folder that holds the csvs that hold provider urls")
arg_parser.add_argument(dest="destination", action="store", help="The location you would like the data from this run to be stored")
arg_parser.add_argument(dest="run_name", action="store", help="the name of this run (will be used to save files under")
args = arg_parser.parse_args()

# example URLS 
# example_index_1 = 'https://groupaccess.deltadentalil.com/cmsdata/index.json'

# example_provider_1 = 'https://groupaccess.deltadentalil.com/cmsdata/data/providers.json'

# Directory = r"\\dc1fs\INTWORK\CSSIP\Data\Health_Data\Data\Episode3_URLS"
# DESTINATION = r"\\dc1fs\INTWORK\CSSIP\Data\Health_Data\Data\Episode3_CSVS"


Directory = args.url_directory

# if a different path was chosen for the destination, use that one instead of the url path 
DESTINATION = args.destination


# initizlize Some Log Files 
log_file = pd.DataFrame()
bad_url_data = pd.DataFrame()

# hit the destinatnation to make sure it works 
test_file = open(os.path.join(DESTINATION, "temp.txt"), 'w')
test_file.write("testing 1 2 3")
test_file.close()


def __MAIN__():
	"""
	The heart and soul of this progject


	"""
	# set these as global so we can write them in real time and 
	# write them in case of error
	global log_file
	global bad_url_data


	### SOME GLOB CODE 
	directory_path = os.path.join(Directory, '*.csv')
	print directory_path
	for csv_file in iglob(directory_path):

		print "In File" + str(csv_file)

		# get the provider_ID from the direcorty path (the first five numbers from the csv file name)
		this_provider_ID = str(csv_file)[-22: -17]

		#Used to seperate urls in the same file
		url_num = 1

		# Load file 
		current_provider_list = pd.read_csv(csv_file)

		for index, row in current_provider_list.iterrows():
			this_provider_url = row['PROVIDER_URL']
			print this_provider_url

			# Update provider ID to reflect the number of the number of the url 
			this_url_provider_ID = this_provider_ID + "_URL_" + str(url_num)

			try:
				result_data = regular_expression_parse_key(this_provider_url, this_url_provider_ID, DESTINATION)

				# retureve data  from the result data object 
				this_npi_count_csv = result_data['npi_count']
				this_npi_parsed_count = result_data['npi_parsed']

				
				if result_data['bad_url'] == True:
					bad_url_bool = "True"
					bad_url_data = bad_url_data.append([{"index_number": this_provider_ID, "provider_url": this_provider_url, "npi_count_in_csv": this_npi_count_csv, \
						"npi_count_parsed": this_npi_parsed_count, "Bad_URL": bad_url_bool, 'Error': result_data['error'], 'Url_COMPLETE': result_data["url_completed"]}], ignore_index = True)
				elif result_data["url_completed"] == False:
					bad_url_bool = "True"
					bad_url_data = bad_url_data.append([{"index_number": this_provider_ID, "provider_url": this_provider_url, "npi_count_in_csv": this_npi_count_csv, \
						"npi_count_parsed": this_npi_parsed_count, "Bad_URL": bad_url_bool, 'Error': result_data['error'], 'Url_COMPLETE': result_data["url_completed"]}], ignore_index = True)
				else:
					bad_url_bool = None

				log_file = log_file.append([{"index_number": this_provider_ID, "provider_url": this_provider_url, "npi_count_in_csv": this_npi_count_csv, \
					"npi_count_parsed": this_npi_parsed_count, "Bad_URL": bad_url_bool, 'Url_COMPLETE': result_data["url_completed"]}], ignore_index = True)

				# increase URL num for the next itteration
				url_num += 1 
			except Exception as e:
				bad_url_data = bad_url_data.append([{"index_number": this_provider_ID, "provider_url": this_provider_url, "npi_count_in_csv": "", \
						"npi_count_parsed": "", "Bad_URL": True, 'Error': e, 'Url_COMPLETE': False}], ignore_index = True)
				log_file = log_file.append([{"index_number": this_provider_ID, "provider_url": this_provider_url, "npi_count_in_csv": "", "npi_count_parsed": "", "Bad_URL": 'True', 'Url_COMPLETE': False}], ignore_index = True)
				print "Error in Main Loop: ", e 


		

	log_file.to_csv(os.path.join(DESTINATION, str(args.run_name) + "_log_file.csv"))
	bad_url_data.to_csv(os.path.join(DESTINATION, str(args.run_name) + "_bad_urls.csv"))

	#pprint.pprint(log_file)	

try:
	__MAIN__()

except Exception as e:
	log_file.to_csv(os.path.join(DESTINATION, str(args.run_name) + "_log_file.csv"))
	bad_url_data.to_csv(os.path.join(DESTINATION, str(args.run_name) + "_bad_urls.csv"))
	message = twiloClient.messages.create(body = 'WARNING WILL ROBINSON!!!: ' + str(e), from_ = mytwilioNumber, to=myCellPhone)
	print e
	print PrintException()
#__MAIN__()

print "COMPLETE"