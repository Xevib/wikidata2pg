#!/usr/bin/env python
import psycopg2
from psycopg2.extras import Json
from decimal import Decimal
import json
from datetime import datetime
import sys
import traceback
import re

class WikiData(object):
    def _dec2float(self,d):
        if type(d) == dict:
            for k in d:
                if type(d[k]) == Decimal:
                    d[k] = float(d[k])
        return d

    def __init__(self, file,host, database, user, password, postgis):
        self.file = file
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.postgis = postgis
        self.conn = psycopg2.connect(database= self.database,user= self.user,password=password,host=host)
        print "Connected,startirng dump"
        self.entries = {}
        self.sitelinks = {}
        self.start = datetime.now()

    def checkPostgis(self):
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT PostGIS_full_version();")
            return cur.fetchone() !=()
        except:
            return False

    def switchTables(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS wikidata_entities;")
        self.conn.commit()
        cur.execute("DROP TABLE IF EXISTS wikidata_sitelinks;")
        self.conn.commit()
        cur.execute("ALTER TABLE wikidata_entities_tmp RENAME TO wikidata_entities;")
        self.conn.commit()
        cur.execute("ALTER TABLE wikidata_sitelinks_tmp RENAME TO wikidata_sitelinks;")
        self.conn.commit()
        cur.execute("ALTER TABLE public.wikidata_entities RENAME CONSTRAINT wikidata_entities_pkey_tmp TO wikidata_entities_pkey;")
        self.conn.commit()
        cur.execute("ALTER TABLE public.wikidata_sitelinks RENAME CONSTRAINT wikidata_sitelinks_pkey_tmp TO wikidata_sitelinks_pkey;")
        self.conn.commit()
        cur.close()

    def initTemp(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS wikidata_entities_tmp;")
        self.conn.commit()
        cur.execute("DROP TABLE IF EXISTS wikidata_sitelinks_tmp;")
        self.conn.commit()
        sql = "CREATE TABLE public.wikidata_entities_tmp(entity text,statment text,value json,id integer NOT NULL DEFAULT nextval('indx_entity'::regclass),CONSTRAINT wikidata_entities_pkey_tmp PRIMARY KEY (id))"
        cur.execute(sql)
        self.conn.commit()
        sql = "CREATE TABLE public.wikidata_sitelinks_tmp(entity text,lang text,title text,id integer NOT NULL DEFAULT nextval('indx_entity'::regclass),CONSTRAINT wikidata_sitelinks_pkey_tmp PRIMARY KEY (id))"
        cur.execute(sql)
        self.conn.commit()
        cur.execute("SELECT AddGeometryColumn('public','wikidata_entities_tmp','geom','4326','POINT',2);")
        self.conn.commit()
        cur.execute("SELECT 0 as ex FROM pg_class where relname = 'indx_entity'")
        data = cur.fetchall()
        if data == [(0,)]:
            cur.execute("SELECT setval('indx_entity', 1, true)")
        else:
            cur.execute("CREATE SEQUENCE public.indx_entity INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;")
        self.conn.commit()
        cur.close()


    def loadData(self):
        with open(self.file, 'r') as f:
            for line in f:
                if line != "[\n" and line != "]" and line != "]\n" and len(line) > 2:
                    try:
                        if line.endswith(",\n"):
                            item = json.loads(line[:-2])
                        else:
                            item = json.loads(line)
                        item_id = item.get('id')
                        if item_id[0] == 'Q':
                            self.entries[item_id] = {}
                            if 'claims' in item and item['claims'] != []:
                                for property in item['claims'].keys():
                                    value = []
                                    for element in item['claims'][property]:
                                        if element['mainsnak']['snaktype'] == 'value':
                                            value.append({'type': element['mainsnak']['datavalue']['type'],'value': self._dec2float(element['mainsnak']['datavalue']['value'])})
                                    if len(value) == 1:
                                        value = value[0]
                                    self.entries[item_id][property] = value
                        if len(self.entries) > 100:
                            self.saveData()
                            self.entries = {}
                    except Exception as e:
                        print e.message
                        ex_type, ex, tb = sys.exc_info()
                        traceback.print_tb(tb)
                        print line
        print "started at "+str(self.start)
        print "ended at "+str(datetime.now())

    def saveData(self):
        cur = self.conn.cursor()

        for identifier in self.sitelinks.keys():
            links = self.sitelinks[identifier]
            for link in links:
                cur.execute("INSERT INTO wikidata_sitelinks_tmp(entity,lang,title) VALUES (%s,%s,%s)", (identifier,link['lang'],link['title']))
        for identifier in self.entries.keys():
            values = self.entries[identifier]
            for property in values.keys():
                v = json.loads(values[property]['value'])
                try:
                    if type(v) == dict and self.postgis and type(values[property]) != list and 'latitude' in v.keys() and 'longitude' in v.keys():
                        cur.execute("INSERT INTO wikidata_entities_tmp(entity,statment,value,geom) VALUES (%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326))",
                                    (identifier, property, Json(values[property]), v['longitude'], v['latitude'],))
                    else:
                        cur.execute("INSERT INTO wikidata_entities_tmp(entity,statment,value) VALUES (%s,%s,%s)", (identifier,property, Json(values[property]),))
                except Exception as e:
                    print e.message
                    ex_type, ex, tb = sys.exc_info()
                    traceback.print_tb(tb)
                    print "identifier:{}".format(identifier)
                    print "property:{}".format(property)
                    print "values:{}".format(values[property])

        self.conn.commit()
        cur.close()
def help():
    print "Syntax:"
    print "-------"
    print ""
    print "--database=<database> -d=<database> Destination database"
    print "--user=<user> -u=<user> Database user"
    print "--password=<password> -p=<password> Database password"
    print "--host=<host> -h=<host> Database host"
    print "--file=<Wikidata json> -f=<Wikidata json> Wikidata's JSON file"
    print ""
    print "Other commands"
    print "--------------"
    print "--postgis -p Optional , enables postgis usage"
    print "--help -h This message"

postgis_suport = False
database = ""
host = ""
password = ""
user = ""
filename = ""
for arg in sys.argv:
    if arg == '--help' or arg == '-h':
        help()
        exit()
    if arg =='--postgis' or arg == '-p':
        postgis_suport = True
    if re.match('--database=(.*)',arg):
        database = re.match('--database=(.*)',arg).groups()[0]
    if re.match('-d=(.*)',arg):
        database = re.match('-d=(.*)',arg).groups()[0]
    if re.match('--user=(.*)',arg):
        user = re.match('--user=(.*)',arg).groups()[0]
    if re.match('-u=(.*)',arg):
        user = re.match('-u=(.*)',arg).groups()[0]
    if re.match('--password=(.*)',arg):
        password = re.match('--password=(.*)',arg).groups()[0]
    if re.match('-p=(.*)',arg):
        password = re.match('-p=(.*)',arg).groups()[0]
    if re.match('--host=(.*)',arg):
        host = re.match('--host=(.*)',arg).groups()[0]
    if re.match('-h=(.*)',arg):
        host = re.match('-h=(.*)',arg).groups()[0]
    if re.match('--file=(.*)',arg):
        filename = re.match('--file=(.*)',arg).groups()[0]
    if re.match('-f=(.*)',arg):
        filename = re.match('-f=(.*)',arg).groups()[0]

w = WikiData(filename, host, database, user, password,postgis_suport)
if w.checkPostgis() or not postgis_suport:
    w.initTemp()
    w.loadData()
    w.switchTables()
else:
    print "Postgis not installed"
