#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
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


def episode_info_from_data(data):
    episode_info = {}
    pattern = r'^Сериал (.*) \((.*)\): (\d+).* (\d+).*, (.*) \((.*)\). .*'
    re_episode_info = re.match(pattern, data)
    episode_info['show_name_ru'] = re_episode_info.group(1)
    episode_info['show_name'] = re_episode_info.group(2)
    episode_info['season_number'] = int(re_episode_info.group(3))
    episode_info['number'] = int(re_episode_info.group(4))
    episode_info['name_ru'] = re_episode_info.group(5)
    episode_info['name'] = re_episode_info.group(6)
    return episode_info


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
        self.episode_info = {}
        self.feed(requests.get(self.url).text)

    def handle_starttag(self, tag, attrs):
        if not tag == 'meta':
            return
        attrs = dict(attrs)
        if 'property' in attrs and attrs['property'] == 'og:image':
            self.og_image = attrs['content']
        elif 'property' in attrs and attrs['property'] == 'og:description':
            self.og_description = attrs['content']
        elif 'name' in attrs and attrs['name'] == 'description':
            self.episode_info = episode_info_from_data(attrs['content'])


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
        self.pattern = r'^(.*) \((.*)\). (.*). \(S(\d+)E(\d+)\)'

    def online(self):
        if self.feed['status'] == 200:
            return True
        else:
            return False

    def clear_entries(self):
        for entry in self.feed['entries']:
            entry_stamp = {}
            re_entry = re.match(self.pattern, entry['title'])
            entry_stamp['show_name'] = re_entry.group(2)
            entry_stamp['season_number'] = int(re_entry.group(4))
            entry_stamp['number'] = int(re_entry.group(5))
            if entry_stamp not in self.entries_db.values():
                self.new_entry_preparation(entry)
        if self.entries:
            self.entries.reverse()
            return True
        return False

    def new_entry_preparation(self, entry):
        entry_link = entry['link']
        entry_extractor = Extractor(entry_link)
        episode = entry_extractor.episode_info
        entry_timestamp = calendar.timegm(entry['published_parsed'])
        if entry_extractor.og_image:
            entry_pic_episode = entry_extractor.og_image
        else:
            entry_pic_episode = poster_from_data(entry['summary'])
        if entry_extractor.og_description:
            entry_description = 'Описание:\n||' + markdownv2_converter(entry_extractor.og_description) + '||'
        else:
            entry_description = ''
        episode['link'] = entry_link
        episode['description'] = entry_description
        episode['pic'] = entry_pic_episode
        episode['timestamp'] = entry_timestamp
        self.entries.append(episode)

    def send_new_entries(self):
        for entry in self.entries:
            episode = {}
            self.bot.send(entry)
            episode['show_name'] = entry['show_name']
            episode['season_number'] = entry['season_number']
            episode['number'] = entry['number']
            timestamp = self.timestamp_uniq(entry['timestamp'])
            self.entries_db[timestamp] = episode
        self.clear_old_entries()
        with open(self.entries_db_file, 'w', encoding='utf8') as dump_file:
            json.dump(self.entries_db, dump_file, ensure_ascii=False)

    def clear_old_entries(self):
        old_entries = []
        for timestamp in self.entries_db.keys():
            if self.old_entries_frontier > int(timestamp):
                old_entries.append(timestamp)
        if old_entries:
            for timestamp in old_entries:
                del self.entries_db[timestamp]

    def timestamp_uniq(self, timestamp):
        while True:
            if str(timestamp) in self.entries_db.keys():
                timestamp += 1
            else:
                return str(timestamp)


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

    def send(self, episode):
        photo = episode['pic']
        show_name = markdownv2_converter(f'{episode["show_name_ru"]} ({episode["show_name"]})')
        episode_numbers = markdownv2_converter(f'{episode["season_number"]} сезон, {episode["number"]} эпизод')
        if episode["name_ru"]:
            episode_name = markdownv2_converter(f'{episode["name_ru"]} ({episode["name"]})')
        else:
            episode_name = markdownv2_converter(f'{episode["name"]}')
        episode_link = episode['link']
        description = episode['description']
        caption = f'*{show_name}*\n{episode_numbers}:\n[{episode_name}]({episode_link})\n\n{description}'
        self.bot.send_photo(chat_id=self.chatid, photo=photo, caption=caption, parse_mode='MarkdownV2')

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
