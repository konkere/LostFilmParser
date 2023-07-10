#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import cv2
import numpy
import peewee
import requests
from hashlib import sha1
from telebot import TeleBot
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.request import urlopen
from playhouse.db_url import connect
from configparser import ConfigParser
from datetime import datetime, timedelta
from telebot.types import InputMediaPhoto
from feedparser import parse as feed_parse
from playhouse.shortcuts import model_to_dict
from telebot.apihelper import ApiTelegramException


db_proxy = peewee.DatabaseProxy()


def poster_from_data(data):
    poster = data[data.find('http'):]
    poster = poster[:poster.find('image.jpg')]
    poster = poster[:poster.find('icon.jpg')]
    poster = urljoin(poster, 'poster.jpg')
    return poster


def episode_info_from_data(data):
    episode_info = {}
    movie_info = {}
    pattern_episode = r'^(.*) \((.*)\). (\d+) сезон (\d+) серия, (.*?[.]*?) \((.*)\): кадры.*$'
    pattern_movie = r'^(.*) \((.*)\): кадры.*$'
    re_episode_info = re.match(pattern_episode, data)
    if re_episode_info:
        episode_info['show_name_ru'] = re_episode_info.group(1)
        episode_info['show_name'] = re_episode_info.group(2)
        episode_info['season_number'] = int(re_episode_info.group(3))
        episode_info['number'] = int(re_episode_info.group(4))
        episode_info['name_ru'] = re_episode_info.group(5)
        episode_info['name'] = re_episode_info.group(6)
        return episode_info
    else:
        re_movie_info = re.match(pattern_movie, data)
        movie_info['name_ru'] = re_movie_info.group(1)
        movie_info['name'] = re_movie_info.group(2)
        return movie_info


def markdownv2_converter(text):
    symbols_for_replace = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for symbol in symbols_for_replace:
        text = text.replace(symbol, '\\' + symbol)
    return text


def generate_episode_caption(entry):
    if entry["show_name"] == entry["show_name_ru"]:
        show_name = markdownv2_converter(f'{entry["show_name"]}')
    else:
        show_name = markdownv2_converter(f'{entry["show_name_ru"]} ({entry["show_name"]})')
    if entry["season_number"] == 999:
        episode_numbers = markdownv2_converter(f'Спецэпизод {entry["number"]}')
    else:
        episode_numbers = markdownv2_converter(f'{entry["season_number"]} сезон, {entry["number"]} эпизод')
    if not entry['name_ru'] or entry['name_ru'] == entry['name']:
        episode_name = markdownv2_converter(f'{entry["name"]}')
    else:
        episode_name = markdownv2_converter(f'{entry["name_ru"]} ({entry["name"]})')
    episode_link = entry['url']
    if entry['description']:
        description = 'Описание:\n||' + markdownv2_converter(entry['description']) + '||'
    else:
        description = ''
    caption = f'*{show_name}*\n{episode_numbers}:\n[{episode_name}]({episode_link})\n\n{description}'
    return caption


def generate_movie_caption(entry):
    if entry['name'] == entry['name_ru']:
        name = f'{entry["name"]}'
    else:
        name = f'{entry["name_ru"]} ({entry["name"]})'
    movie_link = entry['url']
    description = ''
    if entry['description']:
        description_crop = ''
        crop = 1024 - len(name) - 15
        if crop != len(entry['description']):
            description_crop = entry['description']
            description_crop = description_crop[:crop - 3] + '(...)'
        description = 'Описание:\n||' + markdownv2_converter(description_crop) + '||'
    caption = f'*[{markdownv2_converter(name)}]({movie_link})*\n\n{description}'
    return caption


def generate_schedule_caption(section, schedule):
    date = ''
    episodes = ''
    float_date = False if section == 'сегодня' or section == 'завтра' else True
    title = (
            markdownv2_converter(f'Релизы, запланированные ')
            + ('на ' * (not float_date))
            + f'*{section}*'
            + (markdownv2_converter(f' [{schedule[0]["date"]}]') * (not float_date))
            + markdownv2_converter('.')
    )
    for number, episode in enumerate(schedule, 1):
        number = markdownv2_converter(f'{number}.')
        try:
            is_show = bool(episode["show_name"])
        except KeyError:
            is_show = False
        if is_show:
            if episode["show_name"] == episode["show_name_ru"]:
                show_name = markdownv2_converter(f'{episode["show_name"]}:')
            else:
                show_name = markdownv2_converter(f'{episode["show_name_ru"]} ({episode["show_name"]}):')
            if episode['season_number'] == 999:
                episode_numbers = markdownv2_converter(f'SpE{episode["number"]:02}')
            else:
                episode_numbers = markdownv2_converter(f'S{episode["season_number"]:02}E{episode["number"]:02}')
            if not episode['name_ru'] or episode['name_ru'] == episode['name']:
                episode_name = markdownv2_converter(f'{episode["name"]}')
            else:
                episode_name = markdownv2_converter(f'{episode["name_ru"]} ({episode["name"]})')
            episode_link = episode['url']
            if date == episode['date']:
                pass
            else:
                date = episode['date']
                episodes += ('*' + markdownv2_converter(f'[{date}]:') + '*\n') * float_date
            episodes += f'*{number}* {show_name} {episode_numbers} — [{episode_name}]({episode_link})\n'
        else:
            if not episode['name_ru'] or episode['name_ru'] == episode['name']:
                episode_name = markdownv2_converter(f'{episode["name"]}')
            else:
                episode_name = markdownv2_converter(f'{episode["name_ru"]} ({episode["name"]})')
            episode_link = episode['url']
            if date == episode['date']:
                pass
            else:
                date = episode['date']
                episodes += ('*' + markdownv2_converter(f'[{date}]:') + '*\n') * float_date
            episodes += f'*{number}* [{episode_name}]({episode_link})\n'
    message_text = f'{title}\n\n{episodes}'
    return message_text


def generate_schedule_collage(blank_logo_url, posters_url):
    posters = []
    blank_logo = convert_url2pic(blank_logo_url)
    for url in posters_url:
        poster = convert_url2pic(url)
        posters.append(poster)
    posters_count = len(posters)
    columns = round(posters_count ** .5)
    lines = round_up(posters_count / columns)
    blanks = columns * lines - posters_count
    for _ in range(blanks):
        posters.append(blank_logo)
    horizontal = []
    vertical = []
    poster = 0
    for line in range(lines):
        for column in range(columns):
            horizontal.append(posters[poster])
            poster += 1
        stack_horizontal = numpy.hstack(horizontal)
        vertical.append(stack_horizontal)
        horizontal = []
    numpy_collage = numpy.vstack(vertical)
    is_success, buffer = cv2.imencode(".jpg", numpy_collage)
    collage = buffer.tobytes()
    return collage


def round_up(num):
    num = num * (-1)
    num = num // 1
    num = num * (-1)
    return int(num)


def fingerprint(data):
    data = data.lower()
    hash_data = sha1(data.encode('utf8'))
    hexdigest_hash_data = hash_data.hexdigest()
    return hexdigest_hash_data


def convert_url2pic(url, size=(715, 330)):
    open_url = urlopen(url)
    pic = numpy.asarray(bytearray(open_url.read()), dtype='uint8')
    pic = cv2.imdecode(pic, cv2.IMREAD_COLOR)
    pic = cv2.resize(pic, size)
    return pic


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
        episode['description'] = og_description.replace('&nbsp;', '')
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


class Movies(BaseModel):
    id = peewee.IntegerField()
    date = peewee.DateTimeField()
    name_ru = peewee.CharField()
    name = peewee.CharField()
    description = peewee.TextField()
    url = peewee.CharField()
    poster = peewee.CharField()


class Schedule(BaseModel):
    id = peewee.IntegerField()
    date = peewee.DateTimeField()
    fingerprint = peewee.IntegerField()


class Parser:

    def __init__(self):
        self.settings = Conf()
        self.today_utc = datetime.utcnow().date()
        self.old_entries_frontier = self.today_utc - timedelta(days=self.settings.db_episode_lifetime)
        self.db = connect(self.settings.db_url)
        self.episodes = Episodes
        self.movies = Movies
        self.schedule = Schedule
        db_proxy.initialize(self.db)
        self.db.create_tables([self.episodes, self.movies, self.schedule])
        self.feed = feed_parse(self.settings.rss)
        self.bot = TlgrmBot(self.settings.botid, self.settings.chatid)
        self.new_episodes = []
        self.timetable = {}
        self.pattern = r'^(.*) \((.*)\). (.*). \(S(\d+)E(\d+)\)'
        self.pattern_sp = r'^(.*) \((.*)\). (.*). \((.*) (\d+)\)'
        self.pattern_movie = r'^(.*) \((.*)\). \(Фильм\)$'

    def online(self):
        if self.feed['status'] == 200:
            return True
        else:
            return False

    def check_old_episodes(self):
        for episode in self.episodes.select():
            if episode.date.date() < self.old_entries_frontier:
                episode.delete_instance()
            elif not episode.description or not episode.name_ru or 'poster.jpg' in episode.poster:
                self.check_missed_data(episode)
        for movie in self.movies.select():
            if movie.date.date() < self.old_entries_frontier:
                movie.delete_instance()
            elif not movie.description or not movie.name_ru:
                self.check_missed_data(movie)
        for schedule in self.schedule.select():
            if schedule.date.date() < self.old_entries_frontier:
                schedule.delete_instance()

    def check_missed_data(self, episode):
        need_upd = False
        old_description = episode.description
        old_name_ru = episode.name_ru
        old_poster = episode.poster
        episode_new_check = extractor(episode.url)
        description = episode_new_check['description']
        name_ru = episode_new_check['name_ru']
        poster = episode_new_check['poster']
        if description and not old_description:
            episode.description = description
            need_upd = True
        if name_ru and not old_name_ru:
            episode.name_ru = name_ru
            need_upd = True
        if poster != old_poster:
            self.bot.edit_poster(episode.id, poster)
            episode.poster = poster
            need_upd = True
        if need_upd:
            episode.date = episode.date.date()
            episode_as_dict = model_to_dict(episode)
            try:
                caption = generate_episode_caption(episode_as_dict)
            except KeyError:
                caption = generate_movie_caption(episode_as_dict)
            try:
                self.bot.edit_caption(episode.id, caption)
            except Exception:
                pass
            else:
                episode.save()

    def check_new_entries(self):
        for entry in self.feed['entries']:
            is_show = True
            if ' (Фильм)' in entry['title']:
                elem = self.parse_entry_movie(entry)
                is_show = False
            else:
                elem = self.parse_entry_episode(entry)
            if not self.episode_in_db(elem, is_show):
                try:
                    new_elem = parse_data_from_entry(entry)
                except AttributeError:
                    continue
                else:
                    new_elem['id'] = None
                    new_elem['is_show'] = is_show
                    self.new_episodes.append(new_elem)
        self.new_episodes.reverse()

    def parse_entry_episode(self, entry):
        episode = {}
        try:
            re_entry = re.match(self.pattern, entry['title'])
            episode['season_number'] = int(re_entry.group(4))
        except AttributeError:
            re_entry = re.match(self.pattern_sp, entry['title'])
            episode['season_number'] = 999
        episode['show_name'] = re_entry.group(2)
        episode['number'] = int(re_entry.group(5))
        return episode

    def parse_entry_movie(self, entry):
        movie = {}
        re_entry = re.match(self.pattern_movie, entry['title'])
        movie['name'] = re_entry.group(2)
        return movie

    def send_new_episodes(self):
        for episode in self.new_episodes:
            poster = episode['poster']
            if episode['is_show']:
                caption = generate_episode_caption(episode)
            else:
                caption = generate_movie_caption(episode)
            try:
                message_id = self.bot.send_poster_with_caption(poster, caption).message_id
            except Exception:
                self.new_episodes.remove(episode)
            else:
                episode['id'] = message_id
        if self.new_episodes:
            new_episodes = []
            new_movies = []
            for elem in self.new_episodes:
                if elem['is_show']:
                    del elem['is_show']
                    new_episodes.append(elem)
                else:
                    del elem['is_show']
                    new_movies.append(elem)
            self.episodes.insert_many(new_episodes).execute()
            self.movies.insert_many(new_movies).execute()

    def episode_in_db(self, entry, is_show):
        if is_show:
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
        else:
            try:
                self.movies.get(self.movies.name == entry['name'])
            except self.movies.DoesNotExist:
                return False
            else:
                return True

    def scheduler(self):
        try:
            self.schedule.select().where(self.schedule.date == self.today_utc).get()
        except self.schedule.DoesNotExist:
            response = requests.get(self.settings.schedule)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                sections, blank_logo = self.schedule_parse(response)
                self.send_schedules(sections, blank_logo)
        for entry in self.schedule.select():
            if entry.date.date() < self.old_entries_frontier:
                entry.delete_instance()

    def schedule_parse(self, response):
        divide = ''
        sections = []
        schedule = BeautifulSoup(response.text, features='html.parser')
        blank_logo = schedule.find('meta', property='og:image').get('content')
        schedule_lines = schedule.findAll('tr')
        for line in schedule_lines:
            try:
                divide = line.find('th', {'colspan': 6}).text
            except AttributeError:
                episode = self.schedule_episode(line)
                self.timetable[divide].append(episode)
            else:
                self.timetable[divide] = []
                sections.append(divide)
        return sections, blank_logo

    def schedule_episode(self, episode):
        is_movie = episode.find('div', {'class': 'serie-number-box'}).text
        is_movie = is_movie.join(is_movie.split())
        is_movie = (is_movie == 'Фильм')
        if is_movie:
            pattern_url = r"^goTo\('(\/movies\/.*)',false\);$"
            pattern_date = r'\d{2}.\d{2}.\d{4}'
            column_alpha = episode.find('td', {'class': 'alpha'})
            column_beta = episode.find('td', {'class': 'beta'})
            column_gamma = episode.find('td', {'class': 'gamma'})
            column_delta = episode.find('td', {'class': 'delta'})
            poster = poster_from_data(urljoin('http:', column_alpha.find('img').get('src')))
            re_url = re.match(pattern_url, column_beta.get('onclick'))
            ep_url = urljoin(self.settings.source, re_url[1])
            [name, _, name_ru] = [x.text for x in column_gamma]
            if name_ru:
                name, name_ru = name_ru, name
            ep_date = re.findall(pattern_date, column_delta.text)[0]
            episode = {
                'name': name,
                'name_ru': name_ru,
                'url': ep_url,
                'poster': poster,
                'date': ep_date,
            }
        else:
            pattern_senn = r'^(\d{1,3})[ ]сезон[ ](\d{1,3})[ ]серия$'
            pattern_special = r'^Спецэпизод[ ](\d{1,3})$'
            pattern_url = r"^goTo\('(\/series\/.*)',false\);$"
            pattern_date = r'\d{2}.\d{2}.\d{4}'
            column_alpha = episode.find('td', {'class': 'alpha'})
            column_beta = episode.find('td', {'class': 'beta'})
            column_gamma = episode.find('td', {'class': 'gamma'})
            column_delta = episode.find('td', {'class': 'delta'})
            poster = poster_from_data(urljoin('http:', column_alpha.find('img').get('src')))
            show_name = column_alpha.find('div', {'class': 'en small-text'}).text
            show_name_ru = column_alpha.find('div', {'class': 'ru'}).text
            season_episode = column_beta.find('div', {'class': 'count'}).text
            try:
                re_season_episode = re.match(pattern_senn, season_episode)
                season_number, number = re_season_episode.group(1, 2)
            except AttributeError:
                re_season_episode = re.match(pattern_special, season_episode)
                season_number = 999
                number = re_season_episode.group(1)
            re_url = re.match(pattern_url, column_beta.get('onclick'))
            ep_url = urljoin(self.settings.source, re_url[1])
            [name, _, name_ru] = [x.text for x in column_gamma]
            if name_ru:
                name, name_ru = name_ru, name
            ep_date = re.findall(pattern_date, column_delta.text)[0]
            episode = {
                'show_name': show_name,
                'show_name_ru': show_name_ru,
                'season_number': int(season_number),
                'number': int(number),
                'name': name,
                'name_ru': name_ru,
                'url': ep_url,
                'poster': poster,
                'date': ep_date,
            }
        return episode

    def send_schedules(self, sections, blank_logo):
        if self.timetable:
            for section in sections:
                caption = generate_schedule_caption(section, self.timetable[section])
                caption_fingerprint = fingerprint(caption)
                try:
                    self.schedule.get(self.schedule.fingerprint == caption_fingerprint)
                except self.schedule.DoesNotExist:
                    posters = []
                    for episode in self.timetable[section]:
                        posters.append(episode['poster'])
                    collage = generate_schedule_collage(blank_logo, posters)
                    try:
                        message_id = self.bot.send_poster_with_caption(collage, caption).message_id
                    except ApiTelegramException:
                        message = self.bot.send_poster_with_caption(collage, '')
                        message_id = self.bot.reply_to(message, caption)
                    self.schedule.create(
                        id=message_id,
                        date=self.today_utc,
                        fingerprint=caption_fingerprint
                    )


class Conf:

    def __init__(self):
        self.work_dir = os.path.join(os.getenv('HOME'), '.config', 'LostFilmParser')
        self.config_file = os.path.join(self.work_dir, 'settings.conf')
        self.config = ConfigParser()
        self.exist()
        self.config.read(self.config_file)
        self.botid = self.read('Settings', 'botid')
        self.chatid = self.read('Settings', 'chatid')
        self.source = self.read('System', 'source')
        self.rss = urljoin(self.source, 'rss.xml')
        self.schedule = urljoin(self.source, 'schedule/type_0')
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
        self.config.set('System', 'source', 'https://www.lostfilmtv5.site')
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
        return message

    def edit_caption(self, message_id, caption):
        self.bot.edit_message_caption(
            caption=caption,
            chat_id=self.chatid,
            message_id=message_id,
            parse_mode='MarkdownV2'
        )

    def send_text_message(self, text):
        message = self.bot.send_message(
            chat_id=self.chatid,
            text=text,
            parse_mode='MarkdownV2',
            disable_web_page_preview=True,
        )
        return message.message_id

    def reply_to(self, message, text):
        reply_message = self.bot.reply_to(
            message=message,
            text=text,
            parse_mode='MarkdownV2',
            disable_web_page_preview=True,
        )
        return reply_message.message_id

    def edit_poster(self, message_id, poster):
        self.bot.edit_message_media(
            chat_id=self.chatid,
            message_id=message_id,
            media=InputMediaPhoto(poster),
        )

    def alive(self):
        try:
            self.bot.get_me()
        except Exception:
            return False
        else:
            return True


if __name__ == '__main__':
    lostfilm = Parser()
    if lostfilm.online() and lostfilm.bot.alive():
        lostfilm.check_old_episodes()
        lostfilm.check_new_entries()
        lostfilm.send_new_episodes()
        lostfilm.scheduler()
