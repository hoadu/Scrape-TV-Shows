#!/usr/bin/env python
__author__ = "shubham-jain"

import re
import bleach
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import wptools
from time import sleep
import urllib2
import boto
from andromeda_configuration import AndromedaConfig
from andromeda_helper import upload_image_to_s3
from imdb_data_update import IMDBDetails
from tv_com_scrape import TV_COM
import xlrd
from andromeda_helper import MongoDbConnection
from pymongo import UpdateOne


class WikipediaFetcher(object):

    def __init__(self):
        self.infobox_remove_items = ["Original songs:", "Original songs", "Background Score", "Background Score:","Picture format",
                                     "Songs:", "Lyrics:", "Songs", "Lyrics", "(Soundtrack)","Executive producer(s)","Original network"]

    def fetch(self,page_name):
        """ Passed a Wikipedia page's URL fragment, like 'Edward_Montagu,_1st_Earl_of_Sandwich', this will fetch the page's
        main contents, tidy the HTML, strip out any elements we don't want and return the final HTML string. Returns a dict with two elements:
            'success' is either True or, if we couldn't fetch the page, False.
            'content' is the HTML if success==True, or else an error message."""
        result = self._get_html(page_name)
        if result['success']:
            result['content'] = self._tidy_html(result['content'])
        return result

    def _get_html(self,page_url):
        """Passed the name of a Wikipedia page (eg, 'Samuel_Pepys'), it fetches the HTML content (not the entire HTML page) and returns it.
        Returns a dict with two elements:
            'success' is either True or, if we couldn't fetch the page, False.
            'content' is the HTML if success==True, or else an error message."""
        error_message = ''
        url = page_url
        try:
            response = requests.get(url, params={'action': 'render'},
                                    headers={
                                        "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:50.0) Gecko/20100101 Firefox/50.0"},
                                    timeout=10)
        except requests.exceptions.ConnectionError:
            error_message = "Can't connect to domain."
        except requests.exceptions.Timeout:
            error_message = "Connection timed out."
        except requests.exceptions.TooManyRedirects:
            error_message = "Too many redirects."
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # 4xx or 5xx errors:
            error_message = "HTTP Error: %s" % response.status_code
        except NameError:
            if error_message == '':
                error_message = "Something unusual went wrong."
        if error_message:
            return {'success': False, 'content': error_message}
        else:
            return {'success': True, 'content': response.text}

    def _tidy_html(self,html):
        """ Passed the raw Wikipedia HTML, this returns valid HTML, with all
        disallowed elements stripped out."""
        html = self._bleach_html(html)
        html = self._strip_html(html)
        return html

    def _bleach_html(self,html):
        """ Ensures we have valid HTML; no unclosed or mis-nested tags. Removes any tags and attributes we don't want to let through.
        Doesn't remove the contents of any disallowed tags. Pass it an HTML string, it'll return the bleached HTML string."""
        # Pretty much most elements, but no forms or audio/video.
        allowed_tags = [
            'a', 'abbr', 'acronym', 'address', 'area', 'article', 'b', 'blockquote', 'br', 'caption', 'cite', 'code',
            'col', 'colgroup', 'dd', 'del', 'dfn', 'div', 'dl', 'dt', 'em', 'figcaption', 'figure', 'footer', 'h1',
            'h2', 'h3', 'h4', 'h5', 'h6', 'header', 'hgroup', 'hr', 'i', 'img', 'ins', 'kbd', 'li', 'map', 'nav', 'ol',
            'p', 'pre', 'q', 's', 'samp', 'section', 'small', 'span', 'strong', 'sub', 'sup', 'table', 'tbody', 'td',
            'tfoot', 'th', 'thead', 'time', 'tr', 'ul', 'var',
        ]
        # These attributes will be removed from any of the allowed tags.
        allowed_attributes = {
            '*': ['class', 'id'], 'a': ['href', 'title'], 'abbr': ['title'], 'acronym': ['title'],
            'img': ['alt', 'src', 'srcset'], 'table': ['align'], 'td': ['colspan', 'rowspan'],
            'th': ['colspan', 'rowspan', 'scope'],
        }
        return bleach.clean(html, tags=allowed_tags,
                            attributes=allowed_attributes, strip=True)

    def _strip_html(self,html):
        """Takes out any tags, and their contents, that we don't want at all. And adds custom classes to existing tags (so we can apply CSS styles
        without having to multiply our CSS). Pass it an HTML string, it returns the stripped HTML string."""
        # CSS selectors. Strip these and their contents.
        selectors = [
            'div.hatnote',
            'div.navbar.mini',  # Will also match div.mini.navbar
            # Bottom of https://en.wikipedia.org/wiki/Charles_II_of_England :
            'div.topicon',
            'a.mw-headline-anchor',
        ]
        # Strip any element that has one of these classes.
        classes = [
            # "This article may be expanded with text translated from..."
            # https://en.wikipedia.org/wiki/Afonso_VI_of_Portugal
            'ambox-notice',
            'magnify',
            # eg audio on https://en.wikipedia.org/wiki/Bagpipes
            'mediaContainer',
            'navbox',
            'noprint',
        ]
        # Any element has a class matching a key, it will have the classes
        # in the value added.
        add_classes = {
            # Give these tables standard Bootstrap styles.
            'infobox': ['table', 'table-bordered'],
            'ambox': ['table', 'table-bordered'],
            'wikitable': ['table', 'table-bordered'],
        }
        soup = BeautifulSoup(html,"lxml")
        for selector in selectors:
            [tag.decompose() for tag in soup.select(selector)]

        for clss in classes:
            [tag.decompose() for tag in soup.find_all(attrs={'class': clss})]

        for clss, new_classes in add_classes.iteritems():
            for tag in soup.find_all(attrs={'class': clss}):
                tag['class'] = tag.get('class', []) + new_classes
        # Depending on the HTML parser BeautifulSoup used, soup may have
        # surrounding <html><body></body></html> or just <body></body> tags.
        if soup.body:
            soup = soup.body
        elif soup.html:
            soup = soup.html.body
        # Put the content back into a string.
        html = ''.join(tag.encode('utf-8') for tag in soup.contents)
        return html

    def get_wikidata_for_wiki_page(self, wiki_page_name):
        """Query wikidata, given wikipedia page name
        :param wiki_page_name:
        :return:"""
        try:
            wikidata_result = wptools.page(wiki_page_name, silent=True).get_wikidata()
            if wikidata_result.wikibase:
                wikidata = {
                    "id": wikidata_result.wikibase,
                    "title": wikidata_result.label
                }
                wikidata.update(wikidata_result.wikidata)
                if "instance" in wikidata:
                    del wikidata["instance"]
                return wikidata
        except Exception:
            #print wiki_page_name
            pass
        return {}

    def get_wiki_page_name_from_url(self,wiki_url):
        return wiki_url.replace("http://en.wikipedia.org/wiki/", "").replace("https://en.wikipedia.org/wiki/",
                                                                             "").replace("/", "")
    def replace_names(self, uni_str):
        for remove_item in self.infobox_remove_items:
            uni_str = uni_str.replace(remove_item, "")
        return uni_str.replace(u"\xa0", u" ").replace(u"\u2013", u"-").strip()

    def parse_wiki_infobox(self, wiki_url, html_soup):
        """ Parse infobox in wiki movie page :param html_soup: :return: """
        wiki_page_name = wiki_url.replace("http://en.wikipedia.org/wiki/", "").replace("https://en.wikipedia.org/wiki/","").replace("/", "")
        boto_conn = boto.connect_s3(AndromedaConfig.aws_access_key, AndromedaConfig.aws_secret_key,
                                    host='s3.ap-south-1.amazonaws.com')
        #print AndromedaConfig.s3_decirc_tv_show_images_india_bucket
        s3_decirc_tv_bucket = boto_conn.get_bucket(AndromedaConfig.s3_decirc_tv_show_images_india_bucket)
        infobox_details = {"image": []}
        find_items = ["Directed by", "Starring", "Country of origin","Genre", "No. of seasons" , "No. of episodes", "Original language(s)" , "Related shows" , "Followed by" , "Original release",]
        infobox_table = html_soup.find("table", {"class": "infobox"})
        split_by = ["Directed by", "Starring", "Original release", "Country of origin", "Language"]
        if infobox_table:
            for poster_img in infobox_table.select('a.image > img'):
                infobox_details["image"].append(upload_image_to_s3(s3_decirc_tv_bucket, "andromeda-tv/" + wiki_page_name, "https:" + poster_img["src"],False))
            for poster_img in infobox_table.select('img'):
                infobox_details["image"].append(upload_image_to_s3(s3_decirc_tv_bucket, "andromeda-tv/" + wiki_page_name,
                                       "https:" + poster_img["src"], False))
                infobox_details["image"] = list(set(infobox_details["image"]))
            for infobox_item in infobox_table.find_all("tr"):
                col_key = infobox_item.find("th")
                if col_key and col_key.get_text() in find_items:
                    col_key_text = col_key.get_text().lower().replace(" ", "_").replace(".","")
                    if col_key_text == "casting":
                        col_key_text = "starring"
                    if col_key_text == "original_language(s)":
                        col_key_text = "language"
                    if col_key_text == "original_release":
                        col_key_text = "release_dates"
                    if col_key_text == "country_of_origin":
                        col_key_text = "country"

                    col_val = infobox_item.find("td")
                    if col_val:
                        li_items = col_val.find_all("li")
                        if li_items:
                            infobox_details[col_key_text] = [
                                self.replace_names(li_item.get_text()) for li_item in
                                li_items if
                                li_item.get_text() and (li_item.get_text().strip() not in self.infobox_remove_items)]
                        else:
                            items = re.split(u"<br>|<br/>|,| and ", unicode(col_val))
                            clean_items = []
                            for item in items:
                                child_soup = BeautifulSoup(item,"lxml")
                                for s in child_soup('sup'):
                                    s.extract()
                                # for s in child_soup('span'):
                                #     s.extract()
                                if child_soup.get_text() and (
                                            child_soup.get_text().strip() not in self.infobox_remove_items):
                                    if col_key.get_text() in split_by and "/" in child_soup.get_text():
                                        for split_item in child_soup.get_text().split("/"):
                                            clean_items.append(self.replace_names(split_item))
                                    else:
                                        clean_items.append(self.replace_names(child_soup.get_text()))
                            infobox_details[col_key_text] = clean_items
        return infobox_details

    def parse_wiki_tv_page(self,wiki_url, get_wikidata_data=False, retry=1):
        """
        Parse wikipedia movie page
        :param wiki_url: wikipedia url
        :param get_wikidata_data: get data from wikidata or not
        :return:
        """
        try:
            pass
        except Exception:
            req = urllib2.Request(wiki_url)
            res = urllib2.urlopen(req)
            final_wiki_url = res.geturl()
            sleep(0.5)
            if retry < 4:
                return self.parse_wiki_tv_page(final_wiki_url, get_wikidata_data, retry + 1)
            else:
                #print wiki_url
                return {}
        html = self.fetch(wiki_url)
        html = html['content']
        html = re.sub(">\s*<", "><", html)
        html_soup = BeautifulSoup(html,"lxml")
        # print html_soup.prettify()[1:1000].encode('utf-8')
        # print(html_soup.find(class_='mw-headline'))
        body = html_soup.body.contents
        output = self.parse_wiki_infobox(wiki_url, html_soup)
        output.update({"imdb_id": "", "plot": [], "ext_links": []})
        if get_wikidata_data:
            output["wikidata"] = self.get_wikidata_for_wiki_page(self.get_wiki_page_name_from_url(wiki_url))
        headings = []
        summary_soup_elems = []
        summary_finished = False
        for elem in body:
            # if isinstance(elem, NavigableString):
            # print unicode(elem.string)
            if isinstance(elem, Tag):
                tag = elem.name
                if tag == "p" and not summary_finished:
                    summary_soup_elems.append(elem)
                if tag == 'h2':
                    if elem.text == 'Plot' or elem.text == 'Synopsis':
                        summary_finished = True
                        headings.append(elem.text)
                    if elem.text == 'External links' or elem.text == 'References':
                        headings.append(elem.text)
                if headings and ('Plot' in headings or 'Synopsis' in headings):
                    if tag == 'h2' and elem.text not in headings:
                        headings = []
                    elif tag == 'p':
                        output['plot'].append(elem.text)

                if headings and "External links" in headings or "References" in headings:
                    if tag == 'h2' and elem.text not in headings:
                        headings = []
                    elif tag == 'ul' or tag == 'div':
                        links = elem.find_all("a", class_="external text")
                        if not links:
                            links = elem.find_all("a", class_="external free")

                        for l in links:
                            try:
                                if l['href'].startswith("http://www.imdb.com/title/"):
                                    imdb_ids = re.findall(r"tt\d{7}", l['href'])
                                    if imdb_ids:
                                        output["imdb_id"] = imdb_ids[0]
                                output['ext_links'].append(l['href'])
                                if l['href'].startswith("http://www.tv.com/shows"):
                                    output.update({"TVCOM Link": l['href']})
                            except:
                                pass

        return output

    def parse_all_tv_data(self,show_url,year):
        tv_com_object = TV_COM()
        main_dictionary = {}
        wiki_dictionary = self.parse_wiki_tv_page(show_url, get_wikidata_data=True)
        wiki_dictionary.update({"year": year})
        main_dictionary.update({"wikidata": wiki_dictionary["wikidata"]})
        if "wikidata" in wiki_dictionary: del wiki_dictionary["wikidata"]
        main_dictionary.update({"wikipedia": wiki_dictionary})
        #print "Wiki"

        try:
            imdb_access = IMDBDetails()
            show_imdb_id = wiki_dictionary["imdb_id"]
            if show_imdb_id.startswith("tt"):
                imdb_show_detail = imdb_access.imdb_fetch(show_imdb_id.replace("tt", ""))
                if imdb_show_detail:
                    main_dictionary.update({"imdb":imdb_show_detail})
        except:
            pass
        if "imdb_id" in wiki_dictionary:
            del wiki_dictionary["imdb_id"]

        try:
            tv_com_url = wiki_dictionary["TVCOM Link"]
            if tv_com_url.startswith("http://www.tv.com") or tv_com_url.startswith("https://www.tv.com"):
                tv_com_data = tv_com_object.all_details(tv_com_url)
                main_dictionary.update({"tv_com": tv_com_data})
        except:
            try:
                wiki_data_title = main_dictionary["wikidata"]["title"]
                title = re.sub('[^A-Za-z0-9\s]+', '', wiki_data_title.replace("&","and")).replace(" ", "-").lower()
                tv_test_url = "http://www.tv.com/shows/" + title + "/"
                request = requests.get(tv_test_url)
                if request.status_code == 200:
                    new_tv_com_data = tv_com_object.all_details(tv_test_url)
                    main_dictionary.update({"tv_com": new_tv_com_data})
            except Exception as ex1:
                print(ex1)
                pass
            pass
        if "TVCOM Link" in wiki_dictionary:
            del wiki_dictionary["TVCOM Link"]
        return main_dictionary


    def tv_show_list(self):
        mainData_book = xlrd.open_workbook("Movies.xls", formatting_info=True)
        mainData_sheet = mainData_book.sheet_by_index(0)
        tv_show_list = []
        rows_length = mainData_sheet.nrows
        #(0,rows_length)
        #In case of giving custom order like (150,200) , give a default value to year like year = 2005
        for row in range(0,rows_length):
            rowValues = mainData_sheet.row_values(row, start_colx=0, end_colx=1)
            show_name = rowValues[0]
            try:
                #year = 2005
                year = int(show_name)
            except:
                link = mainData_sheet.hyperlink_map.get((row, 0))
                if link is not None:
                    url = link.url_or_path
                    tv_show_list.append({"year": year, "name": show_name.strip(), "url": url})
                else:
                    print show_name.strip()
                    # print(show_n      ame.ljust(20) + ': ' + url)
                    # print {"year":year,"name":show_name.strip(),"url":url}
        return tv_show_list

    def save_in_mongo(self,mongodb_bulk_data):
        # Bulk query mongo
        mongo_conn = MongoDbConnection(AndromedaConfig.mongo_db_ip_addr)
        mongodb_db_name = "andromeda"
        mongo_db_coll_name = "tv_show_details"
        mongo_conn.bulk_query(mongodb_db_name, mongo_db_coll_name, mongodb_bulk_data)

    def all_scraped_data(self):
        tv_show_wiki_list = self.tv_show_list()
        all_scraped_list = []
        for element in tv_show_wiki_list:
            tv_show_detail = self.parse_all_tv_data(element['url'], element['year'])
            tv_show_detail['wikipedia']['url'] =  element['url']
            criteria = {'wikipedia.url': element['url']}
            all_scraped_list.append(UpdateOne(criteria, {"$set": tv_show_detail}, upsert=True))
        try:
            if all_scraped_list:
                self.save_in_mongo(all_scraped_list)
        except Exception as e:
            print "Mongo Insert failed"
            print (e)
            pass


if __name__ == '__main__':
    '''
    FOR TESTING ONE PARTICULAR TV SHOW
    dic = wiki_fetch.parse_all_tv_data("https://en.wikipedia.org/wiki/Breaking_Bad")
    print dic
    '''
    wiki_fetch = WikipediaFetcher()
    wiki_fetch.all_scraped_data()
