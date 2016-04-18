import requests,csv,os,traceback
from time import sleep

def download_file(url,filename):
    local_filename = filename+'.json'
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    sleep(1)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*64): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename


fd = 'g:/health_data/provider_urls/'
fd2 = '/\dc1fs\intwork\CSSIP\Data\Health_Data\Data\Downloads/'.replace("\\",'/')


log = open('g:/health_data/problems.txt','a')
diri = os.listdir(fd)
outp = csv.writer(open('g:/health_data/crosswalk_URL_downloads.csv','wb'))
outp.writerow(['filename_downloads','URL'])
for d in diri:
    num = 0
    inp = csv.reader(file(fd+d),delimiter='\t')
    for i in inp:
        filename = fd2+d.replace(".csv","")+'_'+str(num)
        outp.writerow([filename,i[0]])
        try:
            name = download_file(i[0],filename)
        except:
            print>>log, d+'\t'+i[0]+'\n'+traceback.print_exc()
        num+=1
log.close()