#!/usr/bin/env python
import psycopg2
from psycopg2.extras import Json
from decimal import Decimal
import json
from datetime import datetime
import sys
import traceback
import re
from StringIO import StringIO
from shapely import geos
from shapely.geometry import Point
import raven
import logging


class WikiData(object):

    @staticmethod
    def _dec2float(d):
        if isinstance(d, dict):
            for k in d:
                if type(d[k]) == Decimal:
                    d[k] = float(d[k])
        return d

    def __init__(self, filename, host, database, user, password, postgis, sentry_dsn=None):
        self.file = filename
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.postgis = postgis
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=password, host=host)
        print 'Connected,startirng dump'
        self.entries = ''
        self.sitelinks = ''
        self.start = datetime.now()
        self.wikire = re.compile('(.*)wiki$')
        self.num_sitelinks =1
        self.num_entries = 1
        geos.WKBWriter.defaults['include_srid'] = True
        self.sentry_dsn = sentry_dsn
        if sentry_dsn:
            self.client = raven.Client(dsn=self.sentry_dsn)

    def check_postgis(self):
        cur = self.conn.cursor()
        try:
            cur.execute('SELECT PostGIS_full_version();')
            return cur.fetchone() !=()
        except:
            return False

    def switch_tables(self):
        cur = self.conn.cursor()
        cur.execute('DROP TABLE IF EXISTS wikidata_entities;')
        self.conn.commit()
        cur.execute('DROP TABLE IF EXISTS wikidata_sitelinks;')
        self.conn.commit()
        cur.execute('ALTER TABLE wikidata_entities_tmp RENAME TO wikidata_entities;')
        self.conn.commit()
        cur.execute('ALTER TABLE wikidata_sitelinks_tmp RENAME TO wikidata_sitelinks;')
        self.conn.commit()
        cur.execute('ALTER TABLE public.wikidata_entities RENAME CONSTRAINT wikidata_entities_pkey_tmp TO wikidata_entities_pkey;')
        self.conn.commit()
        cur.execute('ALTER TABLE public.wikidata_sitelinks RENAME CONSTRAINT wikidata_sitelinks_pkey_tmp TO wikidata_sitelinks_pkey;')
        self.conn.commit()
        cur.close()
        if self.sentry_dsn:
            self.client.captureMessage('Dump finished', level=logging.INFO)

    def init_temp(self):
        if self.sentry_dsn:
            self.client.captureMessage('Dump started', level=logging.INFO)
        cur = self.conn.cursor()
        cur.execute('DROP TABLE IF EXISTS wikidata_entities_tmp;')
        self.conn.commit()
        cur.execute('DROP TABLE IF EXISTS wikidata_sitelinks_tmp;')
        self.conn.commit()
        sql = "CREATE TABLE public.wikidata_entities_tmp(id integer NOT NULL DEFAULT nextval('indx_entity'::regclass),entity text,statment text,value json,CONSTRAINT wikidata_entities_pkey_tmp PRIMARY KEY (id))"
        cur.execute(sql)
        self.conn.commit()
        sql = "CREATE TABLE public.wikidata_sitelinks_tmp(id integer NOT NULL DEFAULT nextval('indx_entity'::regclass),entity text,lang text,title text,CONSTRAINT wikidata_sitelinks_pkey_tmp PRIMARY KEY (id))"
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

    def load_data(self):
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
                            if 'sitelinks' in item and item['sitelinks'] != []:
                                for link in item['sitelinks'].keys():
                                    if self.wikire.match(link):
                                        id = self.num_sitelinks
                                        if item_id is not None:
                                            entity = item_id.replace("\n","\\n").replace("\t", "\\t").replace("\\","\\\\")
                                        else:
                                            entity ="\\N"

                                        if self.wikire.match(link).groups()[0] is not None:
                                            lang = self.wikire.match(link).groups()[0].replace("\n", "\\n").replace("\t","\\t").replace("\\","\\\\")
                                        else:
                                            lang = "\\N"
                                        if item['sitelinks'][link]['title'] is not None:
                                            title = item['sitelinks'][link]['title'].replace("\t", "\\t").replace("\n","\\n").replace("\\","\\\\")
                                        else:
                                            title = "\\N"
                                        dataline = [id, entity, lang, title]
                                        self.sitelinks += '{0}\t{1}\t{2}\t{3}\n'.format(*dataline)
                                        self.num_sitelinks += 1
                            if 'claims' in item and item['claims'] != []:
                                for property in item['claims'].keys():
                                    geom = "\\N"
                                    value = []
                                    for element in item['claims'][property]:
                                        if element['mainsnak']['snaktype'] == 'value':
                                            value.append({'type': element['mainsnak']['datavalue']['type'],'value': self._dec2float(element['mainsnak']['datavalue']['value'])})
                                    if len(value) == 1:
                                        value = value[0]
                                        if isinstance(value['value'], dict) and 'longitude' in value['value'] and 'latitude' in value['value'] and self.postgis:
                                            p = Point(value['value']['longitude'], value['value']['latitude'])
                                            geos.lgeos.GEOSSetSRID(p._geom, 4326)
                                            geom = p.wkb_hex
                                    line = '{0}\t{1}\t{2}\t{3}\t{4}\n'.format(
                                        self.num_entries, item_id, property, Json(value).dumps(value).replace("\\", "\\\\"), geom)
                                    self.entries += line
                                    self.num_entries += 1
                        if len(self.entries) > 1000:
                            self.saveData()
                            self.entries = ''
                            self.sitelinks = ''
                    except Exception as e:
                        if self.sentry_dsn:
                            self.client.captureException()
                        print e.message
                        print traceback.format_exc()
                        print line
        print 'started at '+str(self.start)
        print 'ended at '+str(datetime.now())

    def saveData(self):
        cur = self.conn.cursor()
        ssitelinks = StringIO(self.sitelinks)
        cur.copy_from(ssitelinks, 'wikidata_sitelinks_tmp')
        sentries = StringIO(self.entries)
        cur.copy_from(sentries, 'wikidata_entities_tmp')
        self.conn.commit()
        cur.close()


def help_message():
    print 'Syntax:'
    print '-------'
    print ''
    print '--database=<database> -d=<database> Destination database'
    print '--user=<user> -u=<user> Database user'
    print '--password=<password> -p=<password> Database password'
    print '--host=<host> -h=<host> Database host'
    print "--file=<Wikidata json> -f=<Wikidata json> Wikidata's JSON file"
    print ''
    print 'Other commands'
    print '--------------'
    print '--postgis -p Optional , enables postgis usage'
    print '--sentry-dsn  Optional, uses a sentry dsn to report exceptions'
    print '--help -h This message'


postgis_suport = False
database = ''
host = ''
password = ''
user = ''
filename = ''
dsn = None
for arg in sys.argv:
    if arg == '--help' or arg == '-h':
        help_message()
        exit()
    if arg == '--postgis' or arg == '-p':
        postgis_suport = True
    if re.match('--database=(.*)', arg):
        database = re.match('--database=(.*)', arg).groups()[0]
    if re.match('-d=(.*)',arg):
        database = re.match('-d=(.*)', arg).groups()[0]
    if re.match('--user=(.*)', arg):
        user = re.match('--user=(.*)', arg).groups()[0]
    if re.match('-u=(.*)',arg):
        user = re.match('-u=(.*)', arg).groups()[0]
    if re.match('--password=(.*)', arg):
        password = re.match('--password=(.*)', arg).groups()[0]
    if re.match('-p=(.*)', arg):
        password = re.match('-p=(.*)', arg).groups()[0]
    if re.match('--host=(.*)', arg):
        host = re.match('--host=(.*)', arg).groups()[0]
    if re.match('-h=(.*)',arg):
        host = re.match('-h=(.*)', arg).groups()[0]
    if re.match('--file=(.*)', arg):
        filename = re.match('--file=(.*)', arg).groups()[0]
    if re.match('-f=(.*)',arg):
        filename = re.match('-f=(.*)', arg).groups()[0]
    if re.match('--sentry-dsn=(.*)', arg):
        dsn = re.match('--sentry-dsn=(.*)', arg).groups()[0]

if dsn:
    w = WikiData(filename, host, database, user, password, postgis_suport, sentry_dsn=dsn)
else:
    w = WikiData(filename, host, database, user, password, postgis_suport)
if w.check_postgis() or not postgis_suport:
    w.init_temp()
    w.load_data()
    w.switch_tables()
else:
    print 'Postgis not installed'
