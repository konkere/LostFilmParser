#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import os
import time
import feedparser
from telebot import TeleBot
import requests
from bs4 import BeautifulSoup


# def extract_url(text):
#     extractor = URLExtract()
#     urls = extractor.find_urls(text)
#     return urls[0]


def extract_ep_poster(url):
    url = url.replace('/mr/', '/')
    page = str(BeautifulSoup(requests.get(url).content, 'html.parser'))
    start_link = page.find("/Posters/e_")
    start_quote = page.find('static.lostfilm', start_link)
    end_quote = page.find('.jpg', start_quote)
    url = page[start_quote: end_quote]
    return 'http://' + url + '.jpg'


class ParserRSS:

    def __init__(self, feed):
        self.feed = feedparser.parse(feed)
        self.fresh_timestamp = time.time()
        self.settings = Conf()
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.entries = []

    def online(self):
        if self.feed['status'] == 200:
            return True
        else:
            return False

    def clear_entries(self):
        for entry in self.feed['entries']:
            entry_date_time = entry['published_parsed']
            entry_date_time_unix = time.mktime(entry_date_time)
            if entry_date_time_unix > self.settings.lastupdate:
                entry_name = entry['title']
                entry_link = entry['link']
                entry_name_url = f'[{entry_name}]({entry_link})'
                entry_pic_episode = extract_ep_poster(entry_link)
                # entry_pic = extract_url(entry['summary'])
                self.entries.append([entry_name_url, entry_pic_episode])
            else:
                break
        self.entries.reverse()

    def send_new_entries(self):
        self.settings.write('System', 'lastupdate', f'{self.fresh_timestamp}')
        for entry in self.entries:
            self.bot.send_new(entry[1], entry[0])


class Conf:

    def __init__(self):
        self.work_dir = os.getenv('HOME') + '/.LostFilmRSS'
        self.config_file = self.work_dir + '/settings.conf'
        self.config = configparser.ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read('Settings', 'botid')
        self.chatid = self.read('Settings', 'chatid')
        self.lastupdate = self.read('System', 'lastupdate')

    def exist(self):
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        if not os.path.exists(self.config_file):
            try:
                self.create(self.config_file)
            except FileNotFoundError as exc:
                print(exc)

    def create(self, path):
        self.config.add_section('Settings')
        self.config.add_section('System')
        self.config.set('Settings', 'botid', '000000000:00000000000000000000000000000000000')
        self.config.set('Settings', 'chatid', '00000000000000')
        self.config.set('System', 'lastupdate', '0.0')
        with open(path, 'w') as config_file:
            self.config.write(config_file)
        raise FileNotFoundError(f'Required to fill data in config (section [Settings]): {self.config_file}')

    def read(self, section, setting):
        if setting == 'lastupdate':
            value = self.config.getfloat(section, setting)
        else:
            value = self.config.get(section, setting)
        return value

    def write(self, section, setting, value):
        self.config.set(section, setting, value)
        with open(self.config_file, "w") as config_file:
            self.config.write(config_file)


class TlgrmBot:

    def __init__(self, botid, chatid):
        self.botid = botid
        self.chatid = chatid
        self.bot = TeleBot(self.botid)

    def send_new(self, photo, caption):
        self.bot.send_photo(chat_id=self.chatid, photo=photo, caption=caption, parse_mode="Markdown")


lostfilm = ParserRSS('https://www.lostfilm.run/rss.xml')
if lostfilm.online():
    lostfilm.clear_entries()
    lostfilm.send_new_entries()
