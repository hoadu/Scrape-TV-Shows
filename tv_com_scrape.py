#!/usr/bin/env python
__author__ = "shubham-jain"

from bs4 import BeautifulSoup
import urllib3
import re
from time import sleep
import requests

class TV_COM(object):
    #This function returns the list of all episode url's of a particular show
    def episode_url_func(self,show_url):
        http = urllib3.PoolManager()
        # show_url = "http://www.tv.com/shows/naruto/"
        final_url = []
        # Code to generate an array of seasons from season-1 to season-50
        str1 = 'season-'
        season_array = []
        for num in range(65):
            str2 = str1 + str(num + 1) + '/'
            season_array.append(str2)
        #Append show url to seasons and iterate through different season in order to get final url of tv show
        for season in season_array:
            str1 = show_url + season
            req = requests.get(str1)
            #print "request"
            if req.status_code == 200:
                response = http.request('GET', str1)
                soup = BeautifulSoup(response.data, "html5lib")
                markup = str(soup.body.findAll("a", class_="title")).strip('[]')
                match = re.findall(r'href="[-\w/]+/"', markup)
                for stri in match:
                    stri = stri[5:]
                    stri = 'www.tv.com' + stri[1:-1]
                    final_url.append(str(stri))
        return final_url

    #This function returns a dictionary with all the details of a particular episode
    def episode_detail_func(self,show_url,retry =1):
        http = urllib3.PoolManager()
        response = http.request('GET', show_url)
        soup = BeautifulSoup(response.data, "html.parser")
        # Get season no
        try:
            season_no = 0
            season_Class = soup.find_all("a", class_="ep_season")
            if season_Class:
                season_no = int(season_Class[0].get_text().replace("Season ", ""))
        except:
            season_no = 0
        # Get episode no
        episode_no = 0
        try:
            episode_Class = soup.find_all("span", class_="ep_number")
            if season_Class:
                episode_no = int(episode_Class[0].get_text().replace("Episode ", ""))
        except:
            episode_no = 0
        #if episode_no == 0:
        #    print show_url
        # Rating No
        rating_no = 0
        rating_Class = soup.find_all("div", class_="score")
        if rating_Class:
            try:
                rating_no = float(rating_Class[0].get_text())
            except:
                rating_no = 0
        # date
        date_Class = soup.find_all("div", class_="tagline")
        if date_Class:
            date_str = str(date_Class[0].get_text())
            try:
                month_str = re.findall(r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec', str(date_str))
                month = month_str[0]
            except:
                month = ""
            try:
                date_date = re.findall(r'\d\d,', str(date_str))
                date_date = date_date[0].strip(',')
                date = str(date_date)
            except:
                date = ""
            try:
                year_str = re.findall(r'\d\d\d\d', str(date_str))
                year = str(year_str[0])
            except:
                year = ""
            date_list = date + "-" + month + "-"  + year
        else:
            date_list = "--"
         # Description or Summary
        try:
            description_Class = soup.find_all("div", class_="description")
            if description_Class:
                soupnew = BeautifulSoup(str(description_Class), "html.parser")
                summary = re.sub(r'\\n|\\t|\[|\]|moreless|\\|xa0', '', str(soupnew.get_text()))
            else:
                summary = ""
        except:
            summary = ""
        # Stars List
        stars_list = []
        url_page = [x + 1 for x in range(2)]  # url_page is list of [1,2,3....,10]
        for url in url_page:
            stars_url = show_url + 'cast/stars/' + str(url)
            response = http.request('GET', stars_url)
            stars_soup = BeautifulSoup(response.data, "html.parser")
            stars_class = stars_soup.find_all("div", class_="name")
            for stars in stars_class:
                stars_list.append(stars.get_text().strip())
        # Directors
        director_list = []
        director_url = show_url + 'cast/directors/'
        response = http.request('GET', director_url)
        director_soup = BeautifulSoup(response.data, "html.parser")
        director_class = director_soup.find_all("div", class_="name")
        for director in director_class:
            director_list.append(director.get_text().strip())
        # Title
        try:
            title_str = str(soup.find_all("h2", class_="ep_title")).strip('[]')
            title = str(re.findall(r'>[\w\s?!-:\.]+<', title_str))
            title = title[3:-3]
        except:
            title = ""
            pass

        final_obj = {"title": title, "season": season_no, "episode": episode_no, "rating": rating_no,
                     "date": date_list, "summary": summary, "stars": stars_list, "director": director_list}

        if (episode_no == 0 or rating_no == 0 or season_no == 0 or title == "") and retry < 5:
            sleep(61)
            self.episode_detail_func(show_url,retry+1)
            return final_obj
        else:
            return final_obj

    #This function returns a dictionary of all details inside homepage of a particular show
    def show_details_func(self,show_url,retry = 1):
        http = urllib3.PoolManager()
        response = http.request('GET', show_url)
        soup = BeautifulSoup(response.data, "html.parser")
        try:
            str_title = soup.title.string.strip()
            title = re.sub('- Show News, Reviews, Recaps and Photos - TV.com$', '', str_title)
            title = re.sub('- Episode Guide - TV.com', '', title)
            title = title.strip()
        except:
            title = ""
            pass
        # title is final variable
        try:
            rating_class = soup.find_all("div", class_="score")
            rating_string = str(rating_class)
            match = re.search(r'>[\d.\d]+<', rating_string).group().strip('<>')
            rating_float = float(match)
            # rating_float is final variable
        except:
            rating_float = 0
        category_class = soup.find_all("div", class_="m categories _standard_sub_module")
        category_string = str(category_class)
        txt = re.findall(r'<a[\S\s]+</a', category_string)
        txt = re.findall(r'>[\s\S]+<', str(txt))
        category_str = re.sub(r'</a>|<a href="[\w\s/]+">|\\n|\\t|\\|&amp', '', str(txt))
        category_final = category_str.strip('<>[]\'')
        category_final = category_final.replace(" ;", ",", 99)
        category_final = list(category_final.split(','))
        category_final = [x.strip() for x in category_final]
        # category_final is final variable
        try:
            summary_class = str(soup.find_all("div", class_="description")).strip('[]')
            soupnew = BeautifulSoup(summary_class, "html.parser")
            summary = soupnew.get_text().strip()
            summary_str = re.sub(r'moreless|\\n|\\t|\\|&amp', '', str(summary))
        except:
            summary_str = ""

        # summary_str is final variable
        date_Class = soup.find_all("div", class_="tagline")
        if date_Class:
            date_str = str(date_Class[0].get_text())
            try:
                month_str = re.findall(r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec', str(date_str))
                month = month_str[0]
            except:
                month = ""
            try:
                date_date = re.findall(r'\d\d,', str(date_str))
                date_date = date_date[0].strip(',')
                date = str(date_date)
            except:
                date = ""
            try:
                year_str = re.findall(r'\d\d\d\d', str(date_str))
                year = str(year_str[0])
            except:
                year = ""
            date_str = date + "-" + month + "-" + year
        else:
            date_str = "--"
        #print date_str
        # Star details
        stars_list = []
        url_page = [x + 1 for x in range(2)]  # url_page is list of [1,2,3....,10]
        for url in url_page:
            stars_url = show_url + 'cast/stars/' + str(url)
            response = http.request('GET', stars_url)
            stars_soup = BeautifulSoup(response.data, "html5lib")
            stars_class = stars_soup.find_all("div", class_="name")
            for stars in stars_class:
                stars_list.append(stars.get_text().strip())
        # Director List
        director_list = []
        director_url = show_url + 'cast/directors/'
        response = http.request('GET', director_url)
        director_soup = BeautifulSoup(response.data, "html5lib")
        director_class = director_soup.find_all("div", class_="name")
        for director in director_class:
            director_list.append(director.get_text().strip())
        json_object_show_detail = {'title': title, 'rating': rating_float, 'category': category_final,
                                   'summary': summary_str, 'date': date_str, 'star': stars_list,
                                   'director': director_list}
        if rating_float == 0 and retry < 5:
            sleep(61)
            self.show_details_func(show_url, retry + 1)
            return json_object_show_detail
        else:
            return json_object_show_detail

    #It combines all the details from above three functions and returns a final dictionary
    def all_details(self,show_url,retry=1):
        try:
            episode_detail_json_object = []
            main_json_object = self.show_details_func(show_url)
            #print "main"
            episode_url_list = self.episode_url_func(show_url)
            #print "EPurl"
            for url in episode_url_list:
                episode_detail_json_object.append(self.episode_detail_func(url))
                #print "ep"
            main_json_object.update({'episode_details': episode_detail_json_object})
            return main_json_object
        except Exception as e:
            #print (e)
            sleep(30)
            if retry<4:
                self.all_details(show_url,retry+1)
            else:
                print (e)
                pass


if __name__ == '__main__':
    pass
    """
    #FOR TESTING THIS MODULE
    tv_obj = TV_COM()
    show_details = tv_obj.all_details("http://www.tv.com/shows/bull-2017/")
    #episode_details = tv_obj.episode_detail_func("http://www.tv.com/shows/arrow/disbanded-3449634/")
    print show_details
    """