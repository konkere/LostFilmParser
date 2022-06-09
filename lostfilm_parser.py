#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import peewee
import requests
import feedparser
import configparser
from telebot import TeleBot
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from playhouse.db_url import connect
from playhouse.shortcuts import model_to_dict
from datetime import datetime, timedelta, date as dt_date


db_proxy = peewee.DatabaseProxy()


def poster_from_data(data):
    poster = data[data.find('http'):]
    poster = poster[:poster.find('image.jpg')]
    poster = urljoin(poster, 'poster.jpg')
    return poster


def episode_info_from_data(data):
    episode_info = {}
    pattern = r'^(.*) \((.*)\). (\d+) сезон (\d+) серия, (.*?[.]*?) \((.*)\): кадры.*$'
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


def generate_caption(entry):
    if entry["show_name"] == entry["show_name_ru"]:
        show_name = markdownv2_converter(f'{entry["show_name"]}')
    else:
        show_name = markdownv2_converter(f'{entry["show_name_ru"]} ({entry["show_name"]})')
    if entry["season_number"] == 999:
        episode_numbers = markdownv2_converter(f'Спецэпизод {entry["number"]}')
    else:
        episode_numbers = markdownv2_converter(f'{entry["season_number"]} сезон, {entry["number"]} эпизод')
    if entry["name_ru"]:
        episode_name = markdownv2_converter(f'{entry["name_ru"]} ({entry["name"]})')
    else:
        episode_name = markdownv2_converter(f'{entry["name"]}')
    episode_link = entry['url']
    if entry['description']:
        description = 'Описание:\n||' + markdownv2_converter(entry['description']) + '||'
    else:
        description = ''
    caption = f'*{show_name}*\n{episode_numbers}:\n[{episode_name}]({episode_link})\n\n{description}'
    return caption


def parse_data_from_entry(entry):
    entry_link = entry['link']
    episode = extractor(entry_link)
    entry_date = datetime(*entry['published_parsed'][:3]).date()
    if not episode['poster']:
        episode['poster'] = poster_from_data(entry['summary'])
    episode['date'] = entry_date
    return episode


def extractor(url):
    url = url.replace('/mr/', '/')
    og_image = ''
    og_description = ''
    episode = {}
    response = requests.get(url)
    if response.status_code == 200:
        episode_page = BeautifulSoup(response.text, features='html.parser')
        try:
            og_image = episode_page.find('meta', {'property': 'og:image'}).get('content')
        except AttributeError:
            pass
        try:
            og_description = episode_page.find('meta', {'property': 'og:description'}).get('content')
        except AttributeError:
            pass
        episode = episode_info_from_data(episode_page.title.text)
        episode['poster'] = og_image
        episode['description'] = og_description
        episode['url'] = url
    return episode


class BaseModel(peewee.Model):
    class Meta:
        database = db_proxy


class Episodes(BaseModel):
    id = peewee.IntegerField()
    date = peewee.DateTimeField()
    show_name_ru = peewee.CharField()
    show_name = peewee.CharField()
    season_number = peewee.IntegerField()
    number = peewee.IntegerField()
    name_ru = peewee.CharField()
    name = peewee.CharField()
    description = peewee.TextField()
    url = peewee.CharField()
    poster = peewee.CharField()


class ParserRSS:

    def __init__(self):
        self.settings = Conf()
        self.old_entries_frontier = dt_date.today() - timedelta(days=self.settings.db_episode_lifetime)
        self.db = connect(self.settings.db_url)
        self.episodes = Episodes
        db_proxy.initialize(self.db)
        self.db.create_tables([self.episodes])
        self.feed = feedparser.parse(self.settings.source_rss)
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.new_episodes = []
        self.pattern = r'^(.*) \((.*)\). (.*). \(S(\d+)E(\d+)\)'
        self.pattern_sp = r'^(.*) \((.*)\). (.*). \((.*) (\d+)\)'

    def online(self):
        if self.feed['status'] == 200:
            return True
        else:
            return False

    def check_old_episodes(self):
        for episode in self.episodes.select():
            if episode.date.date() < self.old_entries_frontier:
                episode.delete_instance()
            elif not episode.description:
                self.check_description_update(episode)

    def check_description_update(self, episode):
        description = extractor(episode.url)['description']
        if description:
            episode.description = description
            episode.date = episode.date.date()
            episode_as_dict = model_to_dict(episode)
            caption = generate_caption(episode_as_dict)
            try:
                self.bot.edit_caption(episode.id, caption)
            except Exception:
                pass
            else:
                episode.save()

    def check_new_entries(self):
        for entry in self.feed['entries']:
            episode = {}
            try:
                re_entry = re.match(self.pattern, entry['title'])
                episode['season_number'] = int(re_entry.group(4))
            except AttributeError:
                re_entry = re.match(self.pattern_sp, entry['title'])
                episode['season_number'] = 999
            episode['show_name'] = re_entry.group(2)
            episode['number'] = int(re_entry.group(5))
            if not self.episode_in_db(episode):
                new_episode = parse_data_from_entry(entry)
                new_episode['id'] = None
                self.new_episodes.append(new_episode)
        self.new_episodes.reverse()

    def send_new_episodes(self):
        for episode in self.new_episodes:
            poster = episode['poster']
            caption = generate_caption(episode)
            try:
                message_id = self.bot.send_poster_with_caption(poster, caption)
            except Exception:
                self.new_episodes.remove(episode)
            else:
                episode['id'] = message_id
        if self.new_episodes:
            self.episodes.insert_many(self.new_episodes).execute()

    def episode_in_db(self, entry):
        try:
            self.episodes.get(
                self.episodes.show_name == entry['show_name'],
                self.episodes.season_number == entry['season_number'],
                self.episodes.number == entry['number'],
            )
        except self.episodes.DoesNotExist:
            return False
        else:
            return True


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
        self.db_url = self.db_url_insert_path(self.read('System', 'db'))
        self.db_episode_lifetime = int(self.read('System', 'lifetime'))

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
        self.config.set('System', 'db', 'sqlite:///entries.db')
        self.config.set('System', 'lifetime', '90')
        with open(self.config_file, 'w') as config_file:
            self.config.write(config_file)
        raise FileNotFoundError(f'Required to fill data in config (section [Settings]): {self.config_file}')

    def read(self, section, setting):
        value = self.config.get(section, setting)
        return value

    def db_url_insert_path(self, db_url):
        pattern = r'(^[A-z]*:\/\/\/)(.*$)'
        parse = re.match(pattern, db_url)
        prefix = parse.group(1)
        db_name = parse.group(2)
        path = os.path.join(self.work_dir, db_name)
        db_converted_url = prefix + path
        return db_converted_url


class TlgrmBot:

    def __init__(self, botid, chatid):
        self.botid = botid
        self.chatid = chatid
        self.bot = TeleBot(self.botid)

    def send_poster_with_caption(self, poster, caption):
        message = self.bot.send_photo(
            chat_id=self.chatid,
            photo=poster,
            caption=caption,
            parse_mode='MarkdownV2',
        )
        return message.message_id

    def edit_caption(self, message_id, caption):
        self.bot.edit_message_caption(
            caption=caption,
            chat_id=self.chatid,
            message_id=message_id,
            parse_mode='MarkdownV2'
        )

    def alive(self):
        try:
            self.bot.get_me()
        except Exception:
            return False
        else:
            return True


if __name__ == '__main__':
    lostfilm = ParserRSS()
    if lostfilm.online() and lostfilm.bot.alive():
        lostfilm.check_old_episodes()
        lostfilm.check_new_entries()
        lostfilm.send_new_episodes()
