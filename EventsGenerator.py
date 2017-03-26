import requests
import json, os
from datetime import datetime
from shapely.geometry import MultiPolygon
from shapely.wkt import loads
from shapely.geometry import mapping

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


class generateEvent():
    """
    A class to pull informations about an event
    and also every metadata that goes with it
    """


    def __init__(self, configFilePath):
        if not os.access(configFilePath,
                             os.R_OK):  # check if the config file has the reading permissions set
            print("[CONFIGFILE ERROR] the config file can't be open for reading/writing ")
            exit(0)
        self.config = ConfigParser()
        self.config.read(configFilePath)
        self.configFilePath = configFilePath

        self._elasticSearchHost = self.config.get("elasticsearch", "serverurl")

        self.eventMapping = self.config.get("mapping", "event")
        self.url=self.config.get("resources", "url")






    def getJsonEarthquake(self,id=1070):
        url = self.url+str(id)+".json" #todo check if id is a number and link is working
        re=requests.get(url)
        entriesJsonList=[]
        if re.ok:
            dataJson=re.json()
            eventName=dataJson['name']
            eventName = eventName.split('(')
            magniude=0
            if dataJson['type']!= 'Earthquake':
                return {'jsonEntriesList':False, 'eventname':False}
            try :
                magniude = float(eventName[0].split('-')[0].replace('M ', ''))
                eventName = eventName[1]

            except:
                eventName = eventName[0]

            if magniude < 6:
                return {'jsonEntriesList':False, 'eventname':False}
            for ele in dataJson['entries']:
                entryJson = requests.get(ele)
                if entryJson.ok:
                    entriesJsonList.append(entryJson.json())


            return {'jsonEntriesList':entriesJsonList, 'eventname':eventName}
        return {'jsonEntriesList':False, 'eventname':False}

    def getJson(self,id=1070):
        url = self.url+str(id)+".json" #todo check if id is a number and link is working
        re=requests.get(url)
        entriesJsonList=[]
        if re.ok:
            dataJson=re.json()
            eventName=dataJson['name']


            if dataJson['type'] in ['Forcasts','Earthquake']:
                return {'jsonEntriesList':False, 'eventname':False}
            for ele in dataJson['entries']:
                entryJson = requests.get(ele)
                if entryJson.ok:
                    entriesJsonList.append(entryJson.json())


            return {'jsonEntriesList':entriesJsonList, 'eventname':eventName}
        return {'jsonEntriesList':False, 'eventname':False}



    def getGeom(self, entry):
        geom=None

        if entry['geompoint']:
            geom=entry['geompoint'].split(';')[1]
        elif entry['geommultipolygon']:

            geom = entry['geommultipolygon'].split(';')[1]

        if geom:
            geom=loads(geom) #todo check if geom defined
            #bounds creates  a (minx, miny, maxx, maxy) tuple.
            west,north,east,south=geom.bounds
            BBXgeom = [[west, north], [east, south]]

            return {
            "type": "envelope",
            "coordinates":BBXgeom
        }
        return None

    def getElasticSearchURL(self, index=None, type=None, id=None):
        if None in [index, type, id]:
            return self._elasticSearchHost
        else:
            return self._elasticSearchHost + index + "/" + type+"/" + id

    def setDatasetMapping(self, index, type):
        mapp = {"mappings": {
            type: json.loads(self.eventMapping)
        }
        }
        url = self.getElasticSearchURL()
        po = requests.put(url=url + index, json=mapp)
        return po.content


    def getEventType(self, eventName):
        nameMap={'TOR':"Tornado", 'SVR': "Severe Thunder Storm", 'FFL': 'Flash Flood', 'SMW': "Special Marine Warning"}
        for ele in nameMap.keys():
            if ele in eventName  :
                return nameMap[ele]
            if ')' in eventName:
                return "Earthquake"
        return None
    def getEntryType(self, link):
        nameMap = {'youtube': "Youtube", 'flickr': "Flickr", 'exportKML': 'KML', 'outlook': "Forcast", 'reports': 'Damage Report', 'twitter': 'Tweet'}
        for ele in nameMap.keys():
            if ele in link:
             return nameMap[ele]
        return None
    def getDate(self,stringToDate, eventName=None):
        """

        :param stringToDate:
        :param eventName:
        :return:
        """
        if stringToDate:

            stringToDate = datetime.strptime(stringToDate, "%Y-%m-%dT%H:%M:%SZ")
            return  stringToDate.strftime("%Y-%m-%d %H:%M:%S")
        elif eventName:
            stringToDate="20"+eventName.split(".")[1]
            stringToDate = datetime.strptime(stringToDate, "%Y%m%d%H%M%S")
            return stringToDate.strftime("%Y-%m-%d %H:%M:%S")





    def populateES(self, jsonDataList, index,type,eventName):




        eventType=self.getEventType(eventName)
        if not eventType:
            return
        event_summary = None

        event_description = None
        event_link = None
        event_start_date = None
        event_stop_date = None

        event_BBoX = None




        entries=[]
        entryTypes=[]

        for ele in jsonDataList:

            summary, description, link = (None, None, None)
            if 'summary' in ele.keys():
                summary = ele['summary']
            if 'description' in ele.keys():
                description = ele['description']
            if 'realtime product' in description:
                continue
            link = ele['link']
            if 'realtime product' in description:
                continue

            start_date = self.getDate(stringToDate=ele['start'], eventName=eventName)
            if start_date == None:
                print eventName
            stop_date = self.getDate(stringToDate=ele['stop'], eventName=eventName)

            BBoX = self.getGeom(ele)
            entryType=self.getEntryType(link)
            if entryType :
                entryTypes.append(entryType)

            if eventType=='Earthquake' :

                event_summary = summary

                event_description = ele['description']
                event_link = link

                event_start_date = start_date
                event_stop_date = stop_date

                event_BBoX=BBoX
            else:

                entries.append({'name':ele['name'], 'entryType':self.getEntryType(ele['link']),'link':link, 'start_date':start_date, 'stop_date': stop_date, 'bounding_box':BBoX, 'summary':summary, 'description':description })







        dataToIngest={'name':eventName,'event_type': eventType, 'entries': entries,'entry_types':list(set(entryTypes)), 'summary': event_summary,'description': event_description,'link': event_link, 'bounding_box':event_BBoX, 'start_date':event_start_date, 'stop_date':event_stop_date}
        url = self.getElasticSearchURL(index=index, type=type, id=eventName)
        putReq = requests.post(url=url, json=dataToIngest)
        if putReq.ok == False:
            print putReq.content
            return

    def deleteESIndex(self, index):
        deleteIndex = requests.delete(self._elasticSearchHost + '/' + index )
        return deleteIndex.content



    def  ingestEarthquakes(self, i_start, i_ends):
        for i in range(i_start, i_ends):
            eventName, entriesList = test.getJsonEarthquake(id=i).values()
            if entriesList:
                test.populateES(entriesList, index='event', type='album', eventName=eventName)


    def ingestSeverWeather(self, i_start, i_ends):
        for i in range(i_start, i_ends):
            eventName, entriesList = test.getJson(id=i).values()
            if entriesList:
                test.populateES(entriesList, index='event', type='album', eventName=eventName)

if __name__=="__main__":
    test=generateEvent(configFilePath="configFile.cfg")
    #print test.deleteESIndex("event")
    #print test.setDatasetMapping(index="event", type='album')
    #print test.getJson(id=1070).values()
    #eventName, entriesList = test.getJsonEarthquake(id=1100).values()
    #print entriesList

    #test.ingestEarthquakes(i_start=1099, i_ends=1101)
    test.ingestSeverWeather(i_start=1060, i_ends=1090)
    # for i in range(1099,4200):
    #     eventName, entriesList=test.getJson(id=i).values()
    #     if entriesList:
    #         #print entriesList
    #
    #         test.populateES(entriesList,index='event',type='album',eventName=eventName)

    #print test.getJson()




    #rint m['type']
    #poly=MultiPolygon(test.getJson()['geommultipolygon'])
    #x= test.getAtomFeed("MEGSVR.1605010313")

    #print x.feed['summary']
    #print x.entries[-1]['summary']