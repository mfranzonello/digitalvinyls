''' Streaming music sources and libraries '''

from datetime import datetime
from time import sleep
import re
from urllib import parse

import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

from common.secret import get_secret
from common.structure import SOUNDCLOUD_PLAY_URL, SOUNDCLOUD_WIDGET_URL
from music.dsp import DSP

class Sounder(DSP):
    name = 'SoundCloud'
    play_url = SOUNDCLOUD_PLAY_URL
    widget_url = SOUNDCLOUD_WIDGET_URL
    
    def __init__(self):
        super().__init__()
        self.username = None
        self.driver = None

    def get_user_auths(self, username):
        self.username = username
        
    def connect(self, username=None):
        self.get_user_auths(username)
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--mute-audio')
        self.driver = webdriver.Chrome(options=chrome_options)

    def disconnect(self):
        self.driver.quit()
        
    def get_albums(self):
        self.announce('getting album data')
        albums_df, artists_df, ownerships_df = self.get_releases('albums')
        return albums_df, artists_df, ownerships_df

    def get_favorites(self):
        self.announce('getting favorites data')
        albums_df, artists_df, ownerships_df = self.get_releases('likes')
        return albums_df, artists_df, ownerships_df

    def get_releases(self, page):
        info_albums = {}
        info_artists = {}
        info_ownerships = {}
        
        page_length = 0
        self.driver.get(f'{self.play_url}/{self.username}/{page}')
        elem = self.find_element(self.driver, By.TAG_NAME, 'body')
            
        # fully load page
        while len(self.driver.page_source) > page_length:
            page_length = len(self.driver.page_source)
            elem.send_keys(Keys.PAGE_DOWN)
            sleep(0.2)
        
        playlist_elements = self.find_elements(self.driver, By.CLASS_NAME, 'soundList__item')
        if playlist_elements:
            total_playlists = len(playlist_elements)
            album_urls = []
            for i, playlist_element in enumerate(playlist_elements):
                self.show_progress(i, total_playlists)
                release_element = self.find_element(playlist_element, By.CLASS_NAME, 'sound__trackList')
                    
                # check that it could be a playlist
                if release_element:
                    title_element = self.find_element(playlist_element, By.CLASS_NAME, 'soundTitle__title')
                        
                    # check that there are tracks
                    if self.find_element(playlist_element, By.CLASS_NAME, 'compactTrackList__item'):
                        info__albums = {}
                        info__artists = {}
                        info__ownerships = {}
                        
                        album_url = title_element.get_attribute('href')
                        album_urls.append(album_url)
                        item = self.get_oembed(album_url)
                        artist_uri = self.extract_uri_from_url(item['author_url'])
                        artist_name = item['author_name']
                        date_element = self.find_element(playlist_element, By.CLASS_NAME, 'releaseDateCompact')

                        if date_element:
                            album_type = 'album'
                        else:
                            album_type = 'playlist'
                        
                        info__albums['artist_uris'] = [[artist_uri]]
                        info__albums['album_uri'] = [self.extract_uri_from_html(item['html'], 'playlist')]
                        info__albums['album_name'] = [self.extract_name_from_title(item['title'], artist_name)[0]]
                        info__albums['image_src'] = [item['thumbnail_url']]
                        info__albums['album_type'] = [album_type]
                        ##info__albums['upc'] = [None]

                        info__artists['artist_uri'] = [artist_uri]
                        info__artists['artist_name'] = [artist_name]

                        info__ownerships['album_uri'] = info__albums['album_uri']
                        info__ownerships['like_date'] = [datetime.today().date()]
                        
                        info_albums, info_artists, info_ownerships = \
                            self.combine_infos([info_albums, info_artists, info_ownerships],
                                               [info__albums, info__artists, info__ownerships])
                        
            self.show_progress()
                
            existing_keys = info__albums.keys()
            for i, album_url in enumerate(album_urls):
                self.show_progress(i, total_playlists)
                info__tracks = self.get_tracklist(album_url)
                info_albums = self.combine_infos(info_albums, info__tracks, existing_keys, i)

            self.show_progress()
                                
        albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])
        
        return albums_df, artists_df, ownerships_df

    def get_tracklist(self, album_url):
        self.announce('analyzing albums')
        self.driver.get(album_url)
        sleep(2)
                    
        # check if there are more tracks
        more_element = self.find_element(self.driver, By.CLASS_NAME, 'compactTrackList__moreLink')
        if more_element:
            more_element.click()
            
        title_elements = self.find_elements(self.driver, By.CLASS_NAME, 'trackItem__trackTitle')
        track_uris = []
        for title_element in title_elements:
            track_url = title_element.get_attribute('href')
            item = self.get_oembed(track_url)
            track_uris.append(self.extract_uri_from_html(item['html'], 'track'))
       
        # get album duration
        duration_element = self.find_element(self.driver, By.CLASS_NAME, 'genericTrackCount__duration') # MM:SS
        if duration_element.text:
            minutes, seconds = duration_element.text.split(':')
            album_duration = int(minutes) + int(seconds)/60 
        else:
            album_duration = None
        
        # look for the release date
        time_element = self.find_element(self.driver, By.CLASS_NAME, 'relativeTime')
        listen_element = self.find_element(self.driver, By.CLASS_NAME, 'listenInfo__releaseData') # DD mmm YYY
        if time_element:
            r_date = time_element.get_attribute('title') # or datetime="2019-03-04T20:03:55.000Z"
        elif listen_element:
            r_date = listen_element.text
        release_date = datetime.strptime(r_date,  '%d %B %Y').date()
        
        # update album type to soundtrack if it's in the tag
        tag_element = self.find_element(self.driver, By.CLASS_NAME, 'sc-tag')
        album_type = None
        if tag_element:
            if 'soundtrack' in tag_element.text.lower():
                album_type = 'soundtrack'
        
        info__tracks = {}
        info__tracks['track_uris'] = track_uris
        info__tracks['album_duration'] = album_duration
        info__tracks['release_date'] = release_date
        if album_type:
            info__tracks['album_type'] = album_type

        return info__tracks        

    def get_tracks_data(self, tracks_df):
        self.announce('getting track data')
        
        info_tracks = {}
        max_rows = len(tracks_df)
        for i, track_uri in enumerate(tracks_df['track_uri']):
            self.show_progress(i, max_rows)
            info__tracks = {}
            # use widget to get the url
            self.driver.get(f'{self.widget_url}/tracks/{track_uri}')
            sleep(1)
            button_element = self.find_element(self.driver, By.CLASS_NAME, 'soundHeader__shareButton')
            button_element.click()
            link_code_input = self.find_element(self.driver, By.CLASS_NAME, 'sharePanel__linkCodeInput')
            
            if link_code_input:
                # get trackname and artist info from oembed
                link_code = link_code_input.get_attribute('value')
                url = link_code[:link_code.index('?utm_source')]
                item = self.get_oembed(url)
                artist_name = item['author_name']
                track_name, true_artist_name = self.extract_name_from_title(item['title'], artist_name)
                
                if artist_name == true_artist_name:
                    artist_uri = self.extract_uri_from_url(item['author_url'])
                else:
                    artist_uri = self.get_artist_uri(true_artist_name)

                # get track duration from webpage
                self.driver.get(link_code[:link_code.index('?utm_source')])
                sleep(1)
                timeline_element = self.find_element(self.driver, By.CLASS_NAME, 'playbackTimeline__duration')
                span_element = timeline_element.find_element(By.XPATH, './/span[@aria-hidden="true"]')
                minutes, seconds = span_element.text.split(':')
                duration = int(minutes) + int(seconds)/60

                info__tracks['track_uri'] = [track_uri]
                info__tracks['track_name'] = [track_name]
                info__tracks['artist_uris'] = [[artist_uri]]
                info__tracks['track_duration'] = [duration]
                
                info_tracks = self.combine_infos(info_tracks, info__tracks)

        self.show_progress()
        tracks_df = self.get_df_from_info(info_tracks, subset=['track_uri'])
        
        return tracks_df
    
    def get_artist_uri(self, artist_name):
        self.driver.get(f'{self.play_url}/search/people?q={parse.quote(artist_name)}')
        sleep(1)

        # search through results for verified or most popular
        verified = False
        follows = []
        people_items = self.find_elements(self.driver, By.CLASS_NAME, 'searchList__item')
        for people_item in people_items:
            if self.find_element(people_item, By.CLASS_NAME, 'verifiedBadge'):
                verified = True
                break
            mini_stats_element = self.find_element(people_item, By.CLASS_NAME, 'sc-ministats-item')
            follower_count = int(mini_stats_element.get_attribute('title')[:-len(' followers')].replace(',', ''))
            follows.append(follower_count)
        if not verified:
            people_item = people_items[follows.index(max(follows))]
        
        link_element = self.find_element(people_item, By.CLASS_NAME, 'sc-link-primary')
        artist_uri = link_element.get_attribute('href')[:-len(self.play_url + ' ')]

        return artist_uri
        
    def get_artist_data(self, artists_df):
        self.announce('getting artist data')
        
        info_artists = {}
        max_rows = len(artists_df)
        for i, artist_uri in enumerate(artists_df['artist_uris']):
            info__artists = {}
            self.show_progress(i, max_rows)

            url = f'{self.play_url}/{artist_uri}'
            item = self.get_oembed(url)

            info__artists['artist_uri'] = [artist_uri]
            info__artists['artist_name'] = [item['author_name']]

            info_artists = self.combine_infos(info_artists, info__artists)

        self.show_progress()
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        
        return artists_df
                        
    def find_element(self, parent, by, name):
        return self.check_elements(parent, by, name, multiple=False)

    def find_elements(self, parent, by, name):
        return self.check_elements(parent, by, name, multiple=True)
    
    def check_elements(self, parent, by, name, multiple):
        try:
            if multiple:
                elements = parent.find_elements(by, name)
            else:
                elements = parent.find_element(by, name)
        except NoSuchElementException:
            elements = None
        return elements

    def get_oembed(self, url):
        response = requests.get(url=f'{self.play_url}/oembed', params={'url': url, 'format': 'json'})
        if response.ok:
            item = response.json()
        else:
            item = {}
        return item
        
    def extract_uri_from_html(self, html, uri_type):
        uri = html[html.index(f'{uri_type}s%2F')+len(f'{uri_type}s%2F'):html.index('&show_artwork')]
        return uri

    def extract_uri_from_url(self, url):
        uri = url[url.rindex('/')+1:]
        return uri
        
    def extract_name_from_title(self, title, artist_name): ##, url=None):
        # remove SoundCloud add-on
        name = title[:-len(' by ' + artist_name)]
              
        # remove artist name
        artist_patterns = [rf'^{re.escape(artist_name)} - (.+)$',  # matches 'artist - song title'
                           rf'^{re.escape(artist_name)}\s+(.+)$',  # matches 'artist  song title'
                           r'^(.+?)\s{3}(.+)$',                    # matches 'other artist   song title'
                           r'^(.+)$',                              # matches 'song title'
                           ]
        number_patterns = [rf'^{re.escape(artist_name)} - (.+)$',  # matches 'artist - song title'
                           rf'^{re.escape(artist_name)}\s+(.+)$',  # matches 'artist  song title'
                           r'^(.+?)\s{3}(.+)$',                    # matches 'other artist   song title'
                           r'^(.+)$',                              # matches 'song title'
                           ]

        for pattern in artist_patterns:
            match = re.match(pattern, name)
            if match:
                if pattern == artist_patterns[2]:
                    name = match.group(2)
                    artist_name = match.group(1)  # return song title and true artist
                else:
                    name = match.group(1) # return song title and original artist

        # remove leading track number -> this will overcorrect if it's not actually a track number
        match = re.match(r'^(\d{1,2})\s+(.*)', name)
        if match:
            number = int(match.group(1))
            if 0 <= number < 30: # assume albums have less than 30 songs
                name = match.group(2)
      
        # # # see if the artist name is in the url
        # # file_name = self.extract_uri_from_url(url)
        # # compare_name = name.replace(' ', '-').lower()
        # # if compare_name in file_name:
        # #     possible_artist = file_name.replace(compare_name, '').replace('-', ' ').strip()
        # #     # remove track number

        return name, artist_name
    
    def get_soundtracks_data(self, tracks_df):
        return tracks_df