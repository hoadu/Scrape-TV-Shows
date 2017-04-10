#!/usr/bin/env python
__author__ = "shubham-jain"

from pymongo import MongoClient
import requests
from bson.code import Code
import ssl
import MySQLdb
import urllib2
import StringIO
from boto.s3.key import Key
import unicodedata
import re
from HTMLParser import HTMLParser
from andromeda_configuration import AndromedaConfig
import boto

# For converting to unicode
def remove_spl_char(entity):
    try:
        entity = unicodedata.normalize('NFKD', unicode(HTMLParser().unescape(entity))). \
            encode('ascii', 'ignore').replace(u'&', u' and ').replace(u"'s", u"s")
        return re.sub(' +', ' ', re.sub(u'[^a-zA-Z0-9 ]', u' ', entity)).strip()
    except:
        return entity


def upload_image_to_s3(bucket, key_name, image_url, overwrite=True):
    """
    :param bucket: Bucket object in boto.s3.bucket (https://s3.ap-south-1.amazonaws.com/decirc-tv-show-images-india/)
    :param key_name: andromeda/Star_(1982_film)
    :param image_url: https://upload.wikimedia.org/wikipedia/en/thumb/3/38/Star-1982-1.JPG/220px-Star-1982-1.JPG
    :param overwrite: Overwrite existing image
    :return: https://s3.ap-south-1.amazonaws.com/decirc-tv-show-images-india/andromeda/Star_(1982_film).jpg
    """
    try:
        k = Key(bucket)
        k.key = key_name + '.' + image_url.lower().split('.')[::-1][0]
        if not overwrite and k.exists():
            # Don't overwrite
            pass
        else:
            opener = urllib2.build_opener()
            opener.addheaders = [
                ('User-agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0')]
            file_object = opener.open(image_url)
            page_headers = file_object.headers
            content_type = page_headers.getheader('content-type')
            k.set_metadata('Content-Type', content_type)
            fp = StringIO.StringIO(file_object.read())  # Wrap object
            k.set_contents_from_file(fp)
            k.set_acl('public-read')
        public_url = k.generate_url(expires_in=0, query_auth=False)
        return public_url
    except Exception:
        return image_url


def python_requests(request_method, url, data, headers={}):
    return requests.request(method=request_method, url=url, data=data, headers=headers)


class MongoDbConnection:
    """
    MongoDb queries
    """
    def __init__(self, mongo_db_ip_addr, mongo_ssl=AndromedaConfig.mongo_ssl):
        if mongo_ssl:
            self.mongo_client = MongoClient(mongo_db_ip_addr, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
        else:
            self.mongo_client = MongoClient(mongo_db_ip_addr)

    def map_reduce_query(self, db_name, collection_name, map, reduce, out, full_response=False):
        return self.mongo_client[db_name][collection_name].map_reduce(Code(map), Code(reduce), out, full_response)

    def count(self, db_name, collection_name, count_query):
        count_result = self.mongo_client[db_name][collection_name].find(count_query).count()
        return count_result

    def find_one(self, db_name, collection_name, criteria, projection=None):
        return self.mongo_client[db_name][collection_name].find_one(criteria, projection)

    def find_all(self, db_name, collection_name, criteria, projection=None):
        return self.mongo_client[db_name][collection_name].find(criteria, projection)

    def update_one(self, db_name, collection_name, filter_criteria, update_data, upsert):
        return self.mongo_client[db_name][collection_name].update_one(filter_criteria, update_data, upsert=upsert)

    def bulk_query(self, db_name, collection_name, bulk_data):
        return self.mongo_client[db_name][collection_name].bulk_write(bulk_data)


class MySqlConnection():
    """
    MySql database conn
    """

    def __init__(self, host, user, password, db_name):
        self.mysql_conn = MySQLdb.connect(host=host, user=user, passwd=password, db=db_name)

    def insert_update_mysql_query(self, query):
        """
        Insert or update query
        """
        mysql_cursor = self.mysql_conn.cursor()
        try:
            mysql_cursor.execute(query)
            self.mysql_conn.commit()
            self.mysql_conn.close()
            response = "SUCCESS"
        except Exception, ex:
            self.mysql_conn.rollback()
            self.mysql_conn.close()
            response = "ERROR: " + str(ex)
        return response

    def select_mysql_query(self, query):
        """
        Select query
        """
        mysql_cursor = self.mysql_conn.cursor()
        mysql_cursor.execute(query)
        result = mysql_cursor.fetchall()
        self.mysql_conn.close()
        return result

