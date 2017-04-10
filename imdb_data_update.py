from __future__ import print_function
from imdb import IMDb
from time import sleep
__author__ = "shubham-jain"

"""
IN CASE YOU ARE USING MONGODB MODULE ->
from andromeda_helper import MongoDbConnection
from andromeda_configuration import AndromedaConfig
"""

class IMDBDetails:
    """
    def __init__(self):
        try:
            self.access_imdb = IMDb('http', timeout=120)
            self.imdb_field_list = ['rating', 'runtimes', 'year', 'casting department', 'languages', 'votes', 'producer',
                                    'title',
                                    'mpaa', 'top 250 rank', 'country codes', 'language codes', 'cover url', 'genres',
                                    'director',
                                    'akas',
                                    'kind', 'countries', 'plot outline', 'plot', 'cast', 'canonical title',
                                    'long imdb title',
                                    'long imdb canonical title', 'smart canonical title', 'smart long imdb canonical title',
                                    'full-size cover url']
            self.person_list = ['casting department', 'producer', 'director', 'cast']
        except:
            self.access_imdb = IMDb()
            self.imdb_field_list = ['rating', 'runtimes', 'year', 'casting department', 'languages', 'votes',
                                    'producer',
                                    'title',
                                    'mpaa', 'top 250 rank', 'country codes', 'language codes', 'cover url', 'genres',
                                    'director',
                                    'akas',
                                    'kind', 'countries', 'plot outline', 'plot', 'cast', 'canonical title',
                                    'long imdb title',
                                    'long imdb canonical title', 'smart canonical title',
                                    'smart long imdb canonical title',
                                    'full-size cover url']
            self.person_list = ['casting department', 'producer', 'director', 'cast']
            pass
    """

    def imdb_fetch(self, id,retry=1):
        try:
            access_imdb = IMDb('http', timeout=120)
            imdb_field_list = ['rating', 'runtimes', 'year', 'casting department', 'languages', 'votes', 'producer',
                                    'title',
                                    'mpaa', 'top 250 rank', 'country codes', 'language codes', 'cover url', 'genres',
                                    'director',
                                    'akas',
                                    'kind', 'countries', 'plot outline', 'plot', 'cast', 'canonical title',
                                    'long imdb title',
                                    'long imdb canonical title', 'smart canonical title', 'smart long imdb canonical title',
                                    'full-size cover url']
            person_list = ['casting department', 'producer', 'director', 'cast']
        except:
            try:
                access_imdb = IMDb()
                imdb_field_list = ['rating', 'runtimes', 'year', 'casting department', 'languages', 'votes',
                                        'producer',
                                        'title',
                                        'mpaa', 'top 250 rank', 'country codes', 'language codes', 'cover url', 'genres',
                                        'director',
                                        'akas',
                                        'kind', 'countries', 'plot outline', 'plot', 'cast', 'canonical title',
                                        'long imdb title',
                                        'long imdb canonical title', 'smart canonical title',
                                        'smart long imdb canonical title',
                                        'full-size cover url']
                person_list = ['casting department', 'producer', 'director', 'cast']
            except:
                pass
            pass
        try:
            imdb_details = access_imdb.get_movie(id)
            show_details = {}
            show_details['imdb_id'] = 'tt' + id
            for field in imdb_details.keys():
                if field in imdb_field_list:
                    if field in person_list:
                        people = imdb_details[field]
                        person_list = []
                        for person in people:
                            person_list.append(person["name"])
                        show_details[field.replace(" ", "_").replace("-", "_")] = person_list
                    else:
                        show_details[field.replace(" ", "_").replace("-", "_")] = imdb_details[field]
        except Exception as ex:
            sleep(30)
            if retry<4:
                self.imdb_fetch(id,retry+1)
            else:
                print (ex)
                #print (id)
                pass
        return show_details

'''
if __name__ == "__main__":

    imdb_access = IMDBDetails()
    mongo_db_name = "andromeda"
    mongo_coll_name = "tv_show_details"
    mongo_conn = MongoDbConnection(AndromedaConfig.mongo_db_ip_addr)

    """
        db.movie_details.find({"imdb.imdb_id": {$exists: true, $ne: ""}}, {"imdb.imdb_id":1, "_id":0})
        to check:  {"imdb.rating": { "$exists" : true }}
    """

    # get all docs from mongodb
    imdb_id_list = mongo_conn.find_all(mongo_db_name, mongo_coll_name, {"imdb.imdb_id": {"$exists": True, "$ne": ""}},
                                       {"imdb.imdb_id": 1, "_id": 0})

    for show in imdb_id_list:
        show_imdb_id = str(show['imdb']['imdb_id'])
        if show_imdb_id.startswith("tt"):
            imdb_show_detail = imdb_access.imdb_fetch(show_imdb_id.replace("tt", ""))
            if imdb_show_detail:
                filter_criteria = {"imdb.imdb_id": show_imdb_id}
                mongo_conn.update_one(mongo_db_name, mongo_coll_name, filter_criteria,
                                      {"$set": {"imdb": imdb_show_detail}},
                                      upsert=False)



'''



#EXAMPLE OF getting show details
"""
imdb_access = IMDBDetails()
show_imdb_id = 'tt0898266'
if show_imdb_id.startswith("tt"):
    imdb_show_detail = imdb_access.imdb_fetch(show_imdb_id.replace("tt", ""))
    print (imdb_show_detail)
"""


