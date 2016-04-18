import re,csv,json,os,traceback,string,random
from subprocess import call

def id_generator(size=12, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

fd = '/\dc1fs\intwork\CSSIP\Data\Health_Data\Data/'.replace("\\",'/')

### Fields common to all providers
general = ['npi','type','last_updated_on','plans']  # plans is array
### Individual provider fields
individual = ['name','accepting','gender','addresses','speciality','languages']  #addresses, speciality and languages are arrays
### Facility provider fields
facility = ['facility_name','facility_type','addresses']


### List fields
ind_name = ['first','middle','last','suffix']
address = ['address','address_2','city','state','zip','phone']
plans = ['plan_id_type','plan_id','network_tier']

### Construct header row
header = general[:3]+plans+['first','last']+individual[1:3]+address+individual[4:]+facility[:2]+address
diri = os.listdir(fd+'Downloads/')
for d in diri:
    print d
    log = open('g:/health_data/json_problems.txt','a')
    try:
        inp = json.loads(open(fd+'Downloads/'+d).read())
        outp = csv.writer(open(fd+'Parsed/'+d.replace(".json",'')+'.csv','wb'),delimiter='\t')
        outp.writerow(header)
        for i in inp:
            row = []
            for gen in general[:3]:
                row.append(i[gen])
            ### go thru arrays
            adds = {}
            for e,item in enumerate(i['addresses']):
                adds[e] = []
                for add in address:
                    try:
                        adds[e].append(item[add])
                    except:
                        adds[e].append('')
            pls = {}
            for e,item in enumerate(i['plans']):
                pls[e] = []
                for add in plans:
                    try:
                        pls[e].append(item[add])
                    except:
                        pls[e].append('')
            
            ### get individual provider fields
            if i['type'] == 'INDIVIDUAL':
                specl = []
                try:
                    for e,item in enumerate(i['speciality']):
                        specl.append(item)
                except:
                    try:
                        for e,item in enumerate(i['specialty']):
                            specl.append(item)
                    except:
                        specl = ['']
                langs = []
                try:
                    for e,item in enumerate(i['languages']):
                        langs.append(item)
                except:
                    langs = ['']
                first_name = i['name']["first"]
                last_name = i['name']["last"]
                try:
                    first_name+=" "+i['name']['middle']
                except:
                    pass
                try:
                    last_name+=" "+i['name']['suffix']
                except:
                    pass
            else:
                factype = []
                try:
                    for e,item in enumerate(i['facility_type']):
                        factype.append(item)
                except:
                    factype = ['']
            
            ### try to construct rows
            if i['type'] == "INDIVIDUAL":
                for k,v in pls.items():
                    acc = ''
                    gender = ''
                    try:
                        acc = i['accepting']
                    except:
                        pass
                    try:
                        gender = i['gender']
                    except:
                        pass
                    for k1,v1 in adds.items():
                        for v2 in specl:
                            for v3 in langs:
                                outp.writerow(row+v+[first_name,last_name,acc,gender]+v1+[v2,v3])
            else:
                
                for k,v in pls.items():
                    for v1 in factype:
                        for k2,v2 in adds.items():
                            outp.writerow(row+v+['']*12+[i['facility_name']]+[v1]+v2)
                
    except:
        traceback.print_exc()
        print>>log, d
        print>>log, traceback.print_exc()

    log.close()
    