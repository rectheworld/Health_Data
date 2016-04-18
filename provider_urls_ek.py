import re,csv,json,os,traceback
from splinter.browser import Browser
import splinter
from time import sleep
from subprocess import call

fd = '/\dc1fs\intwork\CSSIP\Data\Health_Data\Data/'.replace("\\",'/')

import mechanize,cookielib

br = mechanize.Browser()
br.set_handle_robots(False)  # bypass robots
br.set_handle_refresh(False)  # can sometimes hang without this
br.addheaders = [('User-agent', 'chrome')]  # Some websites demand a user-agent that isn't a robot

# Cookie Jar
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

browser = Browser()
            
#As of March 27, 2016
inp = csv.reader(file(fd+'Complete_list.csv','rb'))
head = inp.next()
for e,i in enumerate(head):
    print e,i

fd2 = 'g:/health_data/provider_urls/'

for i in inp:
    if not re.search("^None|NOT SUBMITTED",i[2]):
        print i[1]
        try:
            outp = csv.writer(open(os.path.join(fd2,i[1]+'.csv'),'wb'),delimiter='\t')
            browser.visit(i[2])
            sleep(1)
            try:
                need = browser.find_by_css('pre')
                proc = json.loads(need[0].text)
            except:
                need = browser.find_by_tag('body')
                proc = json.loads(re.sub('}.*?$','}',need[0].text))
            for p in proc['provider_urls']:
                outp.writerow([p])
            #call('taskkill /F /IM firefox.exe')
        except:
            traceback.print_exc()
