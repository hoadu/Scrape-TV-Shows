#!/usr/bin/env python
__author__ = "shubham-jain"
import operator

# Copy this file to log_analytics_configuration.py and change values accordingly
class AndromedaConfig:
    # Mongo
    mongo_ssl = False
    mongo_db_ip_addr = 'mongodb://localhost:27017'

    aws_access_key = "####################"
    aws_secret_key = "########################################"
    # S3
    s3_decirc_tv_show_images_india_bucket = "decirc-tv-show-images-india"
    #FOR SPECIFYING SUBFOLDER OF MAIN FOLDER "decirc-tv-show-images-india", SPECIFY IT IN THE FUNCTION upload_image_to_s3 in wiki_tvshow_page_fetch file

    def __init__(self):
        pass
