import sys,re,time
from datetime import datetime
from datetime import timedelta
from time import strftime

# fetch event and process them
def processTask():

	k=(datetime.now().strftime('%Y/%m/%d %H:%M'))
	
	if k in processList:
		obj=processList[k]
		for o in obj:
			print("Current time  [ ",k," ] , Event ",'"',o["taskName"],'" Processed ');
		del processList[k]

	if(len(processList)==0):
		sys.exit(1)
	else:
		time.sleep(60)
		processTask() #call process event untill no event left in the queue


# get start time from command line argument

if len(sys.argv)<3:
	stString=datetime.now()
	stString=stString+timedelta(minutes = 1)
	stString=stString.strftime('%Y/%m/%d %H:%M:%S')
	startTime=datetime.strptime(stString,'%Y/%m/%d %H:%M:%S')
	print("time argument is null, program will take next minute as start time")
else:
	startTime=datetime.strptime(sys.argv[2],'%Y/%m/%d %H:%M')

print(startTime)


# read csv file
with open(str(sys.argv[1]),"r") as f:
	next(f)
	lines = [line.strip('\n') for line in f]

i=0;
task1=[]
date1=[]
priority1=[]
for item in lines:
	try:
		task,date,priority = item.split(',')
		task1.insert(i,task)
		date1.insert(i,date)
		priority1.insert(i,int(priority))
		i+=1
	except Exception:
		task,date = item.split(',')
		task1.insert(i,task)
		date1.insert(i,date)
		priority1.insert(i,9999)
		i+=1
	# else:
	# 	print()

task2=[]
for item in task1:
	for x in (re.findall(r'\d+',item)):
		task2.append(int(x))
date1 =[r.replace("\"","").replace(" / ","/").replace("  ","").replace(" : ",":") for r in date1]

dateTime=[datetime.strptime(datestr, '%Y/%m/%d %H:%M') for datestr in date1]



		
#ordring events based on timestamp 
processList={}
for i in range(len(task2)):
	
	processObject={}
	processObject["taskName"]=task1[i]
	processObject["taskNo"]  =task2[i]
	processObject["taskTime"]=dateTime[i]
	processObject["priority"]=priority1[i]
	a=datetime.strftime(dateTime[i],'%Y/%m/%d %H:%M')
	# print(a)
	if a in processList:
		v=processList[a]
		# print(v)
		v.append(processObject)
		processList[a]=v
	else:
		processList[a]=[processObject]

	newlist = sorted(processList[a], key=lambda k: k['priority']) #sort events based on priority
	processList[a]=newlist

# block to remove events with older timestamp
for oldDate in dateTime:
	if oldDate<startTime:
		index=dateTime.index(oldDate)
		print("Process: ",task1[index]," Already expired")
		formatted=oldDate.strftime('%Y/%m/%d %H:%M')
		if formatted in processList:
			del processList[formatted]


#fetch current system time and wait for the start time
currentTime=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
currentTime=datetime.strptime(currentTime,'%Y-%m-%d %H:%M:%S')
print(currentTime,startTime)
if currentTime<=startTime:
	time.sleep(int((startTime-currentTime).total_seconds()))
else:
	print("System time exceeds the start time")
	sys.exit(1)

# call process events
processTask()
