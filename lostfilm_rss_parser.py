#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import calendar
import requests
import feedparser
import configparser
from time import gmtime
from telebot import TeleBot
from urllib.parse import urljoin
from html.parser import HTMLParser


def poster_from_data(data):
    poster = data[data.find('http'):]
    poster = poster[:poster.find('image.jpg')]
    poster = urljoin(poster, 'poster.jpg')
    return poster


def markdownv2_converter(text):
    symbols_for_replace = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for symbol in symbols_for_replace:
        text = text.replace(symbol, '\\' + symbol)
    return text


class Extractor(HTMLParser):

    def __init__(self, url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url.replace('/mr/', '/')
        self.og_image = ''
        self.og_description = ''
        self.feed(requests.get(self.url).text)

    def handle_starttag(self, tag, attrs):
        if not tag == 'meta':
            return
        attrs = dict(attrs)
        if 'property' in attrs and attrs['property'] == 'og:image':
            self.og_image = attrs['content']
        elif 'property' in attrs and attrs['property'] == 'og:description':
            self.og_description = attrs['content']


class ParserRSS:

    def __init__(self):
        self.old_entries_delta = (2678400 * 3)  # one month x3
        self.old_entries_frontier = calendar.timegm(gmtime()) - self.old_entries_delta
        self.settings = Conf()
        self.entries_db_file = os.path.join(self.settings.work_dir, 'entries.db')
        try:
            self.entries_db = json.load(open(self.entries_db_file))
        except FileNotFoundError:
            self.entries_db = {}
        self.feed = feedparser.parse(self.settings.source_rss)
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.entries = []

    def online(self):
        if self.feed['status'] == 200:
            return True
        else:
            return False

    def clear_entries(self):
        trash_entry = "). . ("
        for entry in self.feed['entries']:
            if trash_entry in entry['title']:
                continue
            elif entry['title'] not in self.entries_db:
                self.new_entry_preparation(entry)
        if self.entries:
            self.entries.reverse()
            return True
        return False

    def new_entry_preparation(self, entry):
        entry_name_orig = entry['title']
        entry_timestamp = calendar.timegm(entry['published_parsed'])
        entry_name_converted = markdownv2_converter(entry_name_orig)
        entry_link = entry['link']
        entry_extractor = Extractor(entry_link)
        if entry_extractor.og_image:
            entry_pic_episode = entry_extractor.og_image
        else:
            entry_pic_episode = poster_from_data(entry['summary'])
        if entry_extractor.og_description:
            entry_description = 'Описание:\n||' + markdownv2_converter(entry_extractor.og_description) + '||'
        else:
            entry_description = ''
        entry_caption = f'[{entry_name_converted}]({entry_link})\n\n{entry_description}'
        self.entries.append([entry_name_orig, entry_timestamp, entry_caption, entry_pic_episode])

    def send_new_entries(self):
        for entry in self.entries:
            self.bot.send(entry[3], entry[2])
            self.entries_db[entry[0]] = entry[1]
        self.clear_old_entries()
        with open(self.entries_db_file, 'w', encoding='utf8') as dump_file:
            json.dump(self.entries_db, dump_file, ensure_ascii=False)

    def clear_old_entries(self):
        old_entries = []
        for name, timestamp in self.entries_db.items():
            if self.old_entries_frontier > timestamp:
                old_entries.append(name)
        for name in old_entries:
            del self.entries_db[name]


class Conf:

    def __init__(self):
        self.work_dir = os.path.join(os.getenv('HOME'), '.LostFilmRSS')
        self.config_file = os.path.join(self.work_dir, 'settings.conf')
        self.config = configparser.ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read('Settings', 'botid')
        self.chatid = self.read('Settings', 'chatid')
        self.source_rss = self.read('System', 'source')

    def exist(self):
        if not os.path.isdir(self.work_dir):
            os.mkdir(self.work_dir)
        if not os.path.exists(self.config_file):
            try:
                self.create()
            except FileNotFoundError as exc:
                print(exc)

    def create(self):
        self.config.add_section('Settings')
        self.config.add_section('System')
        self.config.set('Settings', 'botid', '000000000:00000000000000000000000000000000000')
        self.config.set('Settings', 'chatid', '00000000000000')
        self.config.set('System', 'source', 'https://www.lostfilmtv5.site/rss.xml')
        with open(self.config_file, 'w') as config_file:
            self.config.write(config_file)
        raise FileNotFoundError(f'Required to fill data in config (section [Settings]): {self.config_file}')

    def read(self, section, setting):
        value = self.config.get(section, setting)
        return value


class TlgrmBot:

    def __init__(self, botid, chatid):
        self.botid = botid
        self.chatid = chatid
        self.bot = TeleBot(self.botid)

    def send(self, photo, caption):
        self.bot.send_photo(chat_id=self.chatid, photo=photo, caption=caption, parse_mode="MarkdownV2")

    def alive(self):
        try:
            self.bot.get_me()
        except Exception:
            return False
        else:
            return True


lostfilm = ParserRSS()
if lostfilm.online() and lostfilm.bot.alive() and lostfilm.clear_entries():
    lostfilm.send_new_entries()
