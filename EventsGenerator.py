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








    def getJson(self,id=1070):
        url = self.url+str(id)+".json" #todo check if id is a number and link is working
        re=requests.get(url)
        entriesJsonList=[]
        if re.ok:
            dataJson=re.json()
            eventName=dataJson['name']
            if dataJson['type'] in ['Forcasts', 'Earthquake']:
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

    def getElasticSearchURL(self, index=None, type=None):
        if None in [index, type]:
            return self._elasticSearchHost
        else:
            return self._elasticSearchHost + index + "/" + type

    def setDatasetMapping(self, index, type):
        mapp = {"mappings": {
            type: json.loads(self.eventMapping)
        }
        }
        url = self.getElasticSearchURL()
        po = requests.put(url=url + index, json=mapp)
        return po.content


    def getEventType(self, eventName):
        nameMap={'TOR':"Tornado", 'SVR': "Severe Thunder Storm", 'FFL': 'Flash Flood', 'SMW': "Spetial Marine"}
        for ele in nameMap.keys():
            if ele in eventName:
                return nameMap[ele]
        return None
    def getEntryType(self, link):
        nameMap = {'youtube': "Youtube", 'flickr': "Flickr", 'exportKML': 'KML', 'outlook': "Forcast", 'reports': 'Damage Report', 'twitter': 'Tweet'}
        for ele in nameMap.keys():
            if ele in link:
             return nameMap[ele]
        return None


    def populateES(self, jsonDataList, index,type,eventName):


        entryOf = eventName

        eventType=self.getEventType(eventName)



        name, summary, description, link = (None, None, None, None)

        for ele in jsonDataList:


            BBoX=self.getGeom(ele)

            name=ele['name']

            entryType=self.getEntryType(ele['link'])
            

            if 'summary' in ele.keys():
                summary=ele['summary']
            if 'description' in ele.keys():
                description=ele['description']



            link =ele['link']
            datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
            start_date = datetime.strptime(ele['start'], "%Y-%m-%dT%H:%M:%SZ")
            stop_date = datetime.strptime(ele['stop'], "%Y-%m-%dT%H:%M:%SZ")
            start_date=start_date.strftime("%Y-%m-%d %H:%M:%S")
            stop_date=stop_date.strftime("%Y-%m-%d %H:%M:%S")

            dataToIngest={'name':name,'event_type': eventType, 'entry_type': entryType, 'summary': summary,'description': description,'link': link, 'bounding_box':BBoX,'entryof':entryOf, 'start_date':start_date, 'stop_date':stop_date}
            url = self.getElasticSearchURL(index=index, type=type)
            putReq = requests.post(url=url, json=dataToIngest)
            if putReq.ok == False:
                print putReq.content
                return

    def deleteESIndex(self, index):
        deleteIndex = requests.delete(self._elasticSearchHost + '/' + index )
        return deleteIndex.content







if __name__=="__main__":
    test=generateEvent(configFilePath="configFile.cfg")
    #print test.deleteESIndex("event")
    #print test.setDatasetMapping(index="event", type='album')
    for i in range(1000,1050):
        eventName, entriesList=test.getJson(id=i).values()

        if entriesList:
            test.populateES(entriesList,index='event',type='album',eventName=eventName)

    #print test.getJson()




    #rint m['type']
    #poly=MultiPolygon(test.getJson()['geommultipolygon'])
    #x= test.getAtomFeed("MEGSVR.1605010313")

    #print x.feed['summary']
    #print x.entries[-1]['summary']