import requests
import json
from shapely.geometry import MultiPolygon
from shapely.wkt import loads
from shapely.geometry import mapping

class generateEvent():
    """
    A class to pull informations about an event
    and also every metadata that goes with it
    """

    def __init__(self):
        self.url = "https://ed3test.itsc.uah.edu/eventalbums/restapi/geoalbums/"
        self.datasetMap=""""{
            "properties": {
               "bounding_box": {
                  "type": "geo_shape"
               },
               "name": {
                  "type": "string"

               },
               "type": {
                  "type": "string"
               },
               "summary": {
                  "type": "string",
                  "index": "not_analyzed"
               },
               "link": {
                  "type": "string",

               },
               "description": {
                  "type": "string"

               }

            }
         }"""

        print "I was instiated"




    def getJson(self,id=1070):
        url = self.url+str(id)+".json" #todo check if id is a number and link is working
        re=requests.get(url)
        return re.json()

    def getGeom(self, entry):

        if entry['geompoint']:
            geom=entry['geompoint'].split(';')[1]
        elif entry['geommultipolygon']:
            geom = entry['geommultipolygon'].split(';')[1]
        return geom


    def setEStMapping(self, index, type):
        mapp = {"mappings": {
            type: json.loads(self.datasetMap)
        }
        }
        url = "http://localhost:9200/"
        po = requests.put(url=url + index, json=mapp)
        return po.content







if __name__=="__main__":
    test=generateEvent()
    thegm= test.getGeom(test.getJson())
    geom= loads(thegm)
    print geom.bounds
    m = mapping(geom)
    test.setEStMapping("event","album")
    print m['type']
    #poly=MultiPolygon(test.getJson()['geommultipolygon'])
    #x= test.getAtomFeed("MEGSVR.1605010313")

    #print x.feed['summary']
    #print x.entries[-1]['summary']