''' Calling APIs, receiving JSONs and updating JSONs '''

from datetime import datetime, timedelta
import stat
import time
import os
import re
from random import randint

import requests
from bs4 import BeautifulSoup

from .words import Colors

class NoResponse:
    def __init__(self):
        self.ok = False

class Caller:
    methods = {'get': requests.get,
               'post': requests.post,
               'put': requests.put}

    def __init__(self):
        pass
    
    def no_response(self):
        return NoResponse()

    def invoke_api(self, url, method='get', **kwargs):
        content = None
        jason = None

        print_url = self.get_print_url(url)
        
        print(f'Calling {print_url}...')
        try:
            response = self.methods[method](url=url, timeout=10, **kwargs)
            if response.ok:
                content, jason = self.extract_json(response)
                
            else:
                print(f'...call failed with status code {response.status_code} - {response.reason}')
                print(f'URL: {response.url}')
                # # print(f'{response.headers}')
                
        except Exception as e:
            print(f'...call failed due to {e}.')

        return content, jason

    def get_print_url(self, url, max_length=60, middle='...', end_length=10):
        print_url = url.replace('https://', '').replace('http://', '')
        if len(print_url) > max_length:
            if '&' in print_url:
                print_url = print_url[:print_url.index('&')]
            if len(print_url) > max_length:
                print_url = print_url[:max_length-len(middle)-end_length] + middle + print_url[-end_length:]

        return print_url

    def extract_json(self, response):
        if response.ok:
            content = response.content
            jason = response.json() if len(content) and response.headers.get('Content-Type').startswith('application/json') else None
        else:
            content = None
            jason = None

        return content, jason

    def get_token(self, url, refresh_token=None, **kwargs):
        ##token_info = self.invoke_api(url, method='post', **kwargs).json()
        _, token_info = self.invoke_api(url, method='post', **kwargs)

        now = time.time()
        today = datetime.now()

        token_info['expires_at'] = int(now + token_info['expires_in'])
        token_info['expiry'] = (today + timedelta(seconds=token_info['expires_in'])).strftime('%Y-%m-%dT%H:%M:%S')
        if refresh_token:
            token_info['refresh_token'] = refresh_token

        return token_info



class KeyFinder:
    ''' find the client_id that certain services are using but won't give to new devs '''
    @staticmethod
    def mock_browser_headers(url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                   'Accept-Language': 'en-US,en;q=0.9',
                   'Referer': url,
                   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Connection': 'keep-alive',
                   'Upgrade-Insecure-Requests': '1',
                   }
        return headers

    # Function to search for client_id in JavaScript content
    @staticmethod
    def search_client_id(js_url):
        client_id = None
        js_response = requests.get(js_url)
        if js_response.ok:
            js_content = js_response.text
            match = re.search(r'client_id\s*[:=]\s*["\']?([a-zA-Z0-9]+)["\']?', js_content)
            if match:
                client_id = match.group(1)
                
        return client_id
    
    # Custom sorting key
    @staticmethod
    def sort_key(src, pattern):
        basename = os.path.basename(src)
        return (0, int(basename.split('-')[0])) if pattern.search(basename) else (1, 0)

    @staticmethod
    def find_client_id(url):
        # Set up headers to mimic a browser request
        client_id = None
        try:
            # Make an HTTP GET request to fetch the HTML content of the page with headers
            response = requests.get(url, headers=KeyFinder.mock_browser_headers(url))
            if response.ok:
                # Extract and sort JavaScript URLs by the naming pattern
                html_content = response.text

            if html_content:
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(html_content, 'html.parser')

                # Find all <script> tags
                script_tags = soup.find_all('script')

                # Extract the URLs of the JavaScript files
                js_urls = sorted([tag.get('src') for tag in script_tags if tag.get('src')], key=lambda x: KeyFinder.sort_key(x, re.compile(r'^\d*-[\w-]+\.js$')))
        
                for js_url in js_urls:
                    client_id = KeyFinder.search_client_id(js_url)
                    if client_id:
                        break

            else:
                print('Failed to retrieve the page. Status code:', response.status_code)
            
        except requests.exceptions.RequestException as e:
            print(f'Error: {e}')
            
        return client_id


class Printer:
    hashes = 20
    tab_t = '\t'
    def __init__(self):
        self.end = '\n'
        self.under = 0
        self.over = 0
    
    # Function to demonstrate a progress bar
    def show_progress(self, current=None, total=None, message='completed', tabs=1):
        # Print the progress bar without a newline
        if current is None:
            pct = 1
        elif not total:
            pct = 0
        else:
            pct = current/total
        
        pct_i = round(pct * 100)
        pct_h = round(pct * self.hashes)
            
        color = Colors.scale_color(pct, Colors.RED_RGB, Colors.GREEN_RGB, Colors.YELLOW_RGB)

        self.end = '' if pct < 1 else '\n' # only start new line at 100%
        this_print = f'\r{self.tab_t*tabs}[{color}{pct_h * "#":<{self.hashes}}{Colors.END}] {pct_i}% {message}' # what is printing now
        print_cover = f'{this_print:<{self.under + self.over + 2}}'
        print(print_cover, end=self.end) # print and cover up if needed
        if self.end != '\n' and len(print_cover) > len(this_print):
            print((len(print_cover) - len(this_print)) * '\b', end=self.end) # back up over space

        # reset marker
        self.under = len(this_print)
        self.over = 0
        
    def add_text(self, text, space=' ', max_char=50):
        if self.end == '\n':
            print(text) # print as normal
        else:
            print(self.over * '\b', end=self.end) # back up
            ptext = space + text[:max_char] # get new text
            print(ptext, end=self.end) # place new text
            self.over = len(ptext) # keep note of old text
            
    def display_status(self, response):
        self.add_text(f'{response.status_code} {response.reason}')