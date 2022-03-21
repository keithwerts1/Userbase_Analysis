import csv
import glob
import pandas as pd
import time
from collections import Counter
import pickle

#print("Import Complete")


def GetEmployees(x,details):
    racf = x[0]
    EmployeeList = [d for d in details if d[6] == racf]
    return EmployeeList

def GetRepHierarchy(sr,details):
    reporting = [d for d in details if d[0]==sr][0]
    replist = GetEmployees(reporting,details)
    return reporting, replist

def GetFirstLayer(sr,details,HitList):
    reporting, replist = GetRepHierarchy(sr,details)
    l1score = [h[3] for h in HitList if h[0]==reporting[0]][0]
    layer = [str(reporting[1]+" - "+str(l1score)),reporting[0],len(replist),l1score]
    return layer

def AddLayer(layer,details,HitList):
    layerlist = []
    l1racf = layer[-3]
    try:
        reporting, replist = GetRepHierarchy(l1racf,details)
    except:
        underlayer = layer + ['N/A','N/A',0,0]
        layerlist.append(underlayer)
        return layerlist
    if len(replist) > 0:
        for r in replist:
            try:
                rp, rl = GetRepHierarchy(r[0],details)
                l2score = [h[3] for h in HitList if h[0]==r[0]][0]
                underlayer = layer + [str(r[1]+" - "+str(l2score)),r[0],len(rl),l2score]
                layerlist.append(underlayer)
            except:
                underlayer = layer + [r[1],r[0],len(rl),0]
                layerlist.append(underlayer)

    else:
        underlayer = layer + ['N/A','N/A',0,0]
        layerlist.append(underlayer)
    return layerlist

#Remove Logic for HitList here
def GetOrg(sr,HitList):
    masterlayers = []
    layer = GetFirstLayer(sr,details,HitList)
    masterlayers.append(layer)
    xx = 0
    for m in masterlayers:
        if m[-4:] == ['N/A', 'N/A', 0, 0]:
            pass
        elif xx < 100000:
            masterlayers += [l for l in AddLayer(m,details,HitList)]
            if xx % 1000 == 0:
                print(xx)
            xx += 1
        else:
            break
    ml = [m for m in masterlayers if m[-4:] == ['N/A', 'N/A', 0, 0]]
    return ml

def OpenPickle(f):
    with open(f, 'rb') as Create:
        Detail = pickle.load(Create)
        return Detail
def WritePickle(l,f):
    with open(f,'wb') as loader:
        pickle.dump(l,loader)
def ListWriter(l,cl,fname,sheet_name):
    df = pd.DataFrame(l,columns=cl).reset_index()
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        df.to_excel(writer, sheet_name=sheet_name,index=False)
        
def GetLinkList(rlist):
    linklist = []
    countmaster = []
    for file in glob.glob('Data\IISLogs\*'):
        date = file.split('ex')[-1].split('_')[0]
        with open(file) as f:
            for line in f:
                try:
                    linelist = line.split(" ")
                    Date = linelist[0]
                    Hour = linelist[1].split(":")[0]
                    Time = "".join(linelist[1].split(":")[0:2])
                    IP1 = linelist[2]
                    APIType = linelist[3]
                    APIurl = linelist[4]
                    APIParam = linelist[5]
                    APIPort = linelist[6]
                    UserID = linelist[7].split('\\')[-1].upper()
                    IP2 = linelist[8]
                    Browser = linelist[9]
                    Siteurl = linelist[10]
                    if 'OutageMap' in Siteurl:
                        Siteurl = 'OutageMap'
                    if 'OMS/Event/T' in Siteurl:
                        Siteurl = 'SearchEvent'
                    Status = linelist[11]
                    Lag1 = linelist[12]
                    Lag2 = linelist[13]
                    Lag3 = linelist[14]
                    IP3 = linelist[15]
                    if UserID in rlist:
                        linklist.append([UserID,Date,APIurl,APIParam,Siteurl,Time])
                except:
                    pass

    fname = 'Datasets\linklist'
    return linklist


def GetRacfLinkList(rlist):
    linklist = []
    countmaster = []
    for file in glob.glob('Data\IISLogs\*'):
        print(file)
        date = file.split('ex')[-1].split('_')[0]
        with open(file) as f:
            for line in f:
                try:
                    linelist = line.split(" ")
                    Date = linelist[0]
                    Hour = linelist[1].split(":")[0]
                    Time = "".join(linelist[1].split(":")[0:2])
                    IP1 = linelist[2]
                    APIType = linelist[3]
                    APIurl = linelist[4]
                    APIParam = linelist[5]
                    APIPort = linelist[6]
                    UserID = linelist[7].split('\\')[-1].upper()
                    IP2 = linelist[8]
                    Browser = linelist[9]
                    Siteurl = linelist[10]
                    if 'OutageMap' in Siteurl:
                        Siteurl = 'OutageMap'
                    if 'OMS/Event/T' in Siteurl:
                        Siteurl = 'SearchEvent'
                    Status = linelist[11]
                    Lag1 = linelist[12]
                    Lag2 = linelist[13]
                    Lag3 = linelist[14]
                    IP3 = linelist[15]
                    if UserID in rlist:
                        linklist.append([UserID,Date,APIurl,APIParam,Siteurl,Time])
                except:
                    pass
            cl = ['UserID','Date','APIurl','APIParam','Siteurl','Time']
            df = pd.DataFrame(linklist,columns=cl).reset_index()
            m = df.groupby(['Siteurl','UserID'], sort=True)['Date'].count().reset_index()
            m = m.sort_values(by=['UserID'],ascending=False)
            countmaster.append(m)
    
    fname = str('Datasets\linkmaster.xlsx')
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        df = pd.concat(countmaster)
        df.to_excel(writer, sheet_name='Links',index=False)


    return df

#print("Functions Loaded")

def GetCNT(details):
    cnt = Counter()
    for file in glob.glob('Data\IISLogs\*'):
        print(file)
        userlist = []
        with open(file) as f:
            for line in f:
                try:
                    linelist = line.split(" ")
                    try:
                        user = linelist[7].split('\\')[-1].upper()
                        userlist.append(user)
                    except:
                        pass
                except:
                    pass
        for word in userlist:
            cnt[word] += 1
    

    return cnt

def GetHitList(cnt,details):

    HitList = []
    for d in details:
        PersonalScore = 0
        Score = 0
        try:
            PersonalScore = cnt[d[0]]
            Score = cnt[d[0]]
        except:
            pass
        EmployeeList = GetEmployees(d,details)
        for e in EmployeeList:
            try:
                Score += cnt[e[0]]
            except:
                pass
            elist = GetEmployees(e,details)
            for el in elist:
                if el not in EmployeeList:
                    EmployeeList.append(el)
        if Score != 0:
            HitList.append([d[0],str(d[1]+" - "+str(Score)),PersonalScore,Score])
    return HitList


        



def GetLinksByRacf(racf,HitList):
    fname = str('Datasets\\'''+racf+'.xlsx')
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        ml = GetOrg(racf,HitList)
        print(len(ml))
        df = pd.DataFrame(ml).reset_index()
        df.to_excel(writer, sheet_name='Org',index=False)
        leadership = ['UR4P','ER91','ESJ3']
        rlist = []
        for m in ml:
            if racf in m:
                x = 1
                while x < len(m):
                    r = m[x]
                    if r not in rlist and r not in leadership:
                        rlist.append(r)
                    x += 4
        print(len(rlist))               

        linklist = GetLinkList(rlist)
        print(len(linklist))

        cl = ['UserID','Date','APIurl','APIParam','Siteurl','Time']
        df = pd.DataFrame(linklist,columns=cl).reset_index()
        m = df.groupby(['APIurl'], sort=True)['UserID'].count().reset_index()
        m = df.groupby(['Siteurl'], sort=True)['UserID'].count().reset_index()

        m = m.sort_values(by=['UserID'],ascending=False)
        m.to_excel(writer, sheet_name='Links',index=False)

file = 'Data\P2L.csv'
details = pd.read_csv(file,encoding='cp1252').values.tolist()
HitList = OpenPickle('Datasets\HitList')
GetLinksByRacf('UE2Z',HitList)

def GetLinkListByWord(word):
    linklist = []
    countmaster = []
    for file in glob.glob('Data\IISLogs\*'):
        date = file.split('ex')[-1].split('_')[0]
        with open(file) as f:
            for line in f:
                try:
                    linelist = line.split(" ")
                    Date = linelist[0]
                    Hour = linelist[1].split(":")[0]
                    Time = "".join(linelist[1].split(":")[0:2])
                    IP1 = linelist[2]
                    APIType = linelist[3]
                    APIurl = linelist[4]
                    APIParam = linelist[5]
                    APIPort = linelist[6]
                    UserID = linelist[7].split('\\')[-1].upper()
                    IP2 = linelist[8]
                    Browser = linelist[9]
                    Siteurl = linelist[10]
                    if 'OutageMap' in Siteurl:
                        Siteurl = 'OutageMap'
                    if 'OMS/Event/T' in Siteurl:
                        Siteurl = 'SearchEvent'
                    Status = linelist[11]
                    Lag1 = linelist[12]
                    Lag2 = linelist[13]
                    Lag3 = linelist[14]
                    IP3 = linelist[15]
                    if word in Siteurl:
                        linklist.append([UserID,Date,APIurl,APIParam,Siteurl,Time])
                except:
                    pass
    cl = ['UserID','Date','APIurl','APIParam','Siteurl','Time']
    df = pd.DataFrame(linklist,columns=cl).reset_index()
    m = df.groupby(['APIurl'], sort=True)['UserID'].count().reset_index()   
    m = df.groupby(['Siteurl'], sort=True)['UserID'].count().reset_index()
    m.style
    m = m.sort_values(by=['UserID'],ascending=False)
    fname = str('Datasets\\SCADA.xlsx')
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        m.to_excel(writer, sheet_name='SCADA',index=False)

    fname = 'Datasets\linklist'
    return linklist




def GetRoleBreakdown(er,jr,cnt):
    roles = list(set([e[1] for e in er]))
    fname = str('Datasets\\roles_analysis.xlsx')
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        for r in roles:
            print(r)
            dpickle = str('Datasets\dlist - '+r)
            cntpickle = str('Datasets\cnt - '+r)
            llpickle = str('Datasets\linklist - '+r)
            dlist = []
            rlist = [e[0] for e in er if e[1] == r]
            jlist = [j[0] for j in jr if j[1] == r]
            for d in details:
                if d[11] in jlist and d[0] not in rlist:
                    dlist.append(d)
                    rlist.append(d[0])
            cntr = {x:count for x,count in cnt.items() if x in rlist}
            HitListr = GetHitList(cntr,details)
            WritePickle(cntr,cntpickle)
            WritePickle(dlist,dpickle)
            ml = GetOrg('UR4P',HitListr)
            print(len(ml))
            df = pd.DataFrame(ml).reset_index()
            df.to_excel(writer, sheet_name=str('Org_'+r),index=False)
            linklist = GetLinkList(rlist)
            cl = ['UserID','Date','APIurl','APIParam','Siteurl','Time']
            df = pd.DataFrame(linklist,columns=cl).reset_index()
            m = df.groupby(['APIurl'], sort=True)['UserID'].count().reset_index()
            m = df.groupby(['Siteurl'], sort=True)['UserID'].count().reset_index()
            m = m.sort_values(by=['UserID'],ascending=False)
            m.to_excel(writer, sheet_name=str('Links_'+r),index=False)
            WritePickle(m,llpickle)
            
#print("Process Complete")


file = 'Data\employees_in_roles.csv'
er = pd.read_csv(file,encoding='cp1252').values.tolist()
#print(er[0])
file = 'Data\jobs_in_roles.csv'
jr = pd.read_csv(file,encoding='cp1252').values.tolist()
#print(jr[0])
file = 'Data\P2L.csv'
details = pd.read_csv(file,encoding='cp1252').values.tolist()
#print(details[0])
file = 'Data\Current_URL_Breakdown_Unique_Abbrev.csv'
UA = pd.read_csv(file,encoding='cp1252').dropna().values.tolist()
roles = list(set([e[1] for e in er]))
#print(roles)
rlist = [[e[0]] for e in er if e[1] in roles]
jlist = [[j[0]] for j in jr if j[1] in roles]
rlistfull = [[e[0],e[1]] for e in er if e[1] in roles]
jlistfull = [[j[0],j[1]] for j in jr if j[1] in roles]
def CreateRole_a():
    role_a = []
    for d in details:
        #print(d[0])
        rolelist = []
        for r in roles:
            rlist = [e[0] for e in er if e[1] == r]
            jlist = [j[0] for j in jr if j[1] == r]
            for j in jlist:
                if d[11] in jlist and d[0] not in rlist:
                    rlist.append(d[0])
            if d[0] in rlist:
                rolelist.append(r)
        role_a.append([d[0],rolelist])
    return role_a
    WritePickle(role_a,r'Datasets\role_a')
#for r in role_a[0:10]:
    #print(r)
role_a = OpenPickle(r'Datasets\role_a')
clm = OpenPickle(r'Datasets\clm').reset_index().values.tolist()
#m = clm.groupby(['RACF'], sort=True)['Hits'].sum().reset_index()
#print(m.shape)
#print(m.head())
            
        
        



#rlist = list(set([d[0] for d in details]))
#df = GetRacfLinkList(rlist)
#print(df.head())



def CreateMasterPickle():
    l = OpenPickle(r'Datasets\UserLinkMaster')
    toanalyze = []
    linkmaster = []
    for line in l:
        try:
            if line[0] == 'SearchEvent':
                linkmaster.append(line)
            elif line[0] == 'OutageMap':
                linkmaster.append(line)
            elif line[0] == '-':
                pass
            elif line[0].split('/')[-2].upper() == 'DMSREPORTS':
                pass
            elif line[0].split('/')[-1].upper() == 'DMSREPORTS':
                pass
            elif 'http://dbuinet/' in line[0]:
                pass
            elif 'http://intranet.corp.oncor.com' in line[0]:
                pass
            elif line[0].split('/')[-2].upper() == 'DMSREPORTS':
                pass
            elif '.asp' in line[0].lower():
                pass
            elif 'CDN/' in line[0]:
                pass
            elif 'Scripts/' in line[0]:
                pass
            else:
                try:
                    cmaster = max(list(set([u[2] for u in UA if u[2].lower() in line[0].lower()])),key=len)
                    linkmaster.append([cmaster,line[1],line[2]])
                except:
                    toanalyze.append(line)
        except:
            toanalyze.append(line)

    WritePickle(linkmaster,r'Datasets\linkmaster')
    WritePickle(toanalyze,r'Datasets\toanalyze')

#linkmaster = OpenPickle(r'Datasets\linkmaster')
#cl = ['URL','RACF','Hits']
#df = pd.DataFrame(linkmaster,columns=cl).reset_index()
#clm = df.groupby(['URL','RACF'])['Hits'].sum().reset_index().values.tolist()
#clm = df.groupby(['URL','RACF'])['Hits'].sum().reset_index()
#print(clm.shape)

#print(clm.head())
#WritePickle(clm,r'Datasets\clm')
#clmpivot = clm.pivot_table('Hits', 'RACF', 'URL')
#for col in clmpivot.columns:
    #if 'Appt' in col:
        #print(col)
#filepath = 'clm.csv'  
#clm.to_csv(filepath)
#print(clmpivot['SwitchAppt'].dropna())

#WritePickle(clmpivot,r'Datasets\clmpivot')

#clmrole = []
#for c in clm:
    #cx = [cr for cr in c]
    #if c[1] in rlist:
        #cx.append('ROLETBD')
    #else:
        #cx.append('General')
    #clmrole.append(cx)
#gen = list(set([c[1] for c in clmrole if c[3] == 'General']))
#role = list(set([c[1] for c in clmrole if c[3] == 'ROLETBD']))
#print(len(role))
#print(len(gen))
#print(len(clmrole))




#medals = df.pivot_table('no of medals', ['Year', 'Country'], 'medal')






#toanalyze = OpenPickle(r'Datasets\toanalyze')
#print(len(toanalyze))
#for a in toanalyze[0:100]:
    #print(a)




#cnt =  GetCNT(details)
#WritePickle(cnt,'Datasets\cnt')
#cnt = OpenPickle('Datasets\cnt')
#GetRoleBreakdown(er,jr,cnt)


#print("Process Complete")

#HitList = GetHitList(cnt,details)
#WritePickle(HitList,'Datasets\HitList')
#HitList = OpenPickle('Datasets\HitList')

#HitList = GetHitList(er,jr,details)
#WritePickle(HitList,'Datasets\HitList')
#HitList = OpenPickle('Datasets\HitList')
#print("Details and HitList Loaded")
#GetLinksByRacf('U6S5')
#linklist = GetLinkListByWord('Scada')
#print(len(linklist))
#GetRoleBreakdown()

#samp = HitList[0]
#print(samp)
#name = samp[1].split(" - ")[0]
#samp.append(name)
#print(samp)

#file = r'Datasets\U6S5.xlsx'
#data = pd.read_excel(file).values.tolist()
#print(data)

HitList = OpenPickle('Datasets\HitList')
#print(HitList[0])
#cnt = GetCNT(details)


def GetLinksByRacf(racf,HitList):

    ml = GetOrg(racf,HitList)
    rlist = []
    for m in ml:
        if racf in m:
            x = 1
            while x < len(m):
                r = m[x]
                if r not in rlist:
                    rlist.append(r)
                x += 4
    return rlist

#JS = GetLinksByRacf('UEAY',HitList)
#print(len(JS))
#CD = GetLinksByRacf('U7CR',HitList)
#print(len(CD))
#AF = GetLinksByRacf('FDZU',HitList)
#print(len(AF))



#print(len(JS))
#print(JS[0:10])

#clm = OpenPickle(r'Datasets\clm').reset_index()
#JS = OpenPickle(r'Datasets\JS')
#dffilter = clm["RACF"].isin(JS)
#clm = clm[dffilter].values.tolist()
#print(df.head())

#clmmaster = []
#Add Name Back
#name_a = [d[0:2] for d in details]
#url_a = UA[0:]
#for c in clm:
    #emp = c[1:4]
    #try:
        #name = [n[1] for n in details if c[2] == n[0]][0]
        #emp.append(name)
        #if c[2] in JS:
            #emp.append('Jacqui Spicer')
        #elif c[2] in CD:
            #emp.append('Chris Darby')
        #elif c[2] in AF:
            #emp.append('Antonio Flores')
        #else:
            #emp.append('Other')
        
        #try:
            #url = [u[0] for u in url_a if u[2] == c[1]][0]
            #emp.append(url)
        #except:
            
            #print(c[1])
       # clmmaster.append(emp)
    #except:
        #print(c[2])


#fname = str('Datasets\\USERBASE_MASTER.xlsx')
#with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
    #cl = ['URL_Short','User','Hits','Name','URL_Long']
    #df = pd.DataFrame(clmmaster,columns=cl).reset_index()
    #df.to_excel(writer,sheet_name='details',index=False)
    #print(df.head())
    #m = df.groupby(['APIurl'], sort=True)['UserID'].count().reset_index()
    #m = m.sort_values(by=['UserID'],ascending=False)
    #m.to_excel(writer, sheet_name=str('Links_'+r),index=False)



    



#How to Account for Roles on Jacqui's Team
#rlist = [e[0] for e in er if e[1] in roles]
#jlist = [j[0] for j in jr if j[1] in roles]
#for d in details:
    #if d[11] in jlist and d[0] not in rlist:
        #rlist.append(d[0])
def CreateUBM():
    fname = str('Datasets\\USERBASE_MASTER.xlsx')
    with pd.ExcelWriter(fname, datetime_format='MM-DD-YYYY') as writer:
        JS = GetLinksByRacf('UEAY',HitList)
        cl = ['RACF']
        df = pd.DataFrame(JS,columns=cl).reset_index()
        df.to_excel(writer, sheet_name='JS',index=False)
        CD = GetLinksByRacf('U7CR',HitList)
        df = pd.DataFrame(CD,columns=cl).reset_index()
        df.to_excel(writer, sheet_name='CD',index=False)
        AF = GetLinksByRacf('FDZU',HitList)
        df = pd.DataFrame(AF,columns=cl).reset_index()
        df.to_excel(writer, sheet_name='AF',index=False)
        clm = OpenPickle(r'Datasets\clm').reset_index()
        clm.to_excel(writer, sheet_name='CLM',index=False)
        role_a = OpenPickle(r'Datasets\role_a')
        cl = ['RACF','Roles']
        df = pd.DataFrame(role_a,columns=cl).reset_index()
        df.to_excel(writer, sheet_name='Role_a',index=False)
print("Process Complete")
    
    
    
