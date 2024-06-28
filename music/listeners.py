''' Music fans '''

from pandas import isna

from common.words import Texter, Colors
from common.entry import Stroker
from library.wordbank import RemoveWords

class User:
    def __init__(self, user_id, first_name, last_name):
        self.user_id = user_id
        self.first_name = first_name
        self.last_name = last_name
        self.user_ids = {}
        self.image_src = None
        
    def add_services(self, service_user_ids):
        self.user_ids = service_user_ids
        
    def has_service(self, service_name):
        return self.user_ids.get(service_name) is not None

    def get_user_id(self, service_id):
        return self.user_ids[service_id]

    def add_picture(self, image_src):
        self.image_src = image_src
        

class Picker:
    def __init__(self):
        self.users = []

    def add_users(self, users):
        self.users.extend(users)

    def select_user(self, message=''):
        user_print = '\n'.join(f'[{i+1}] - {user.first_name} {user.last_name}' for i, user in enumerate(self.users))
        user_range = [str(i+1) for i in range(len(self.users))] # need to limit to 9 at a time
        
        if len(self.users) == 0:
            print('No users set up yet.')
            user = None

        if len(self.users) == 1:
            user = self.users[0]
            
        else:
            print(f'Whose music do you want to {message}?')
            print(user_print)
            
            key, _ = Stroker.get_keystroke(allowed_keys=user_range, quit_key='Q')
            
            user = self.users[user_range.index(key)]
            
        return user
        

class Ranker(Texter, Picker):
    def __init__(self):
        Texter.__init__(self)
        Picker.__init__(self)
    
    def play(self, neon):
        user = self.select_user(message='rank and rate')
        user_name = f'{user.first_name} {user.last_name}'
        user_id = user.user_id
        
        print(f'Welcome {user_name}!')
        
        loop = True
        while loop:
            print(f'What would you like to do: {Colors.BLUE}[1]{Colors.END} rank albums, '
                  f'{Colors.RED}[2]{Colors.END} rate albums or {Colors.YELLOW}[3]{Colors.END} see results? '
                  f'Press {Colors.GREY}[Q]{Colors.END} to quit.')

            key, loop = Stroker.get_keystroke(allowed_keys=['1', '2', '3'], quit_key='Q')
            
            if key in ['1', '2']:
                match key:
                    case '1':
                        self.rank_albums(neon, user_id)
                    case '2':
                        self.rate_albums(neon, user_id)
                
            if key in ['1', '2', '3']:
                self.get_summary(neon, user_id)

        # make sure to update results in the materialized views
        print('refreshing materialized view')
        neon.refresh_views()
        print(f'See you later, {user_name}!')
       
    def get_summary(self, neon, user_id):
        albums_df = neon.get_album_summary(user_id, max_ranking=5)
        for category in albums_df['category'].unique():
            print(f'Top {Colors.CYAN}{category}{Colors.END} releases: ')
            print(albums_df.query('category == @category')[['ranking', 'artist_names', 'album_name']])
            print('Press any key to continue...')
            
            key, loop = Stroker.get_keystroke()
            
    def rank_albums(self, neon, user_id):
        loop = True
        while loop:
            # get albums to rank
            print('...pulling up albums to compare...\n')
            albums_df = neon.get_album_comparisons(user_id)
            if not albums_df.empty:
                # user has albums to compare in this category
                artist_names_a, album_name_a, ranking_a = albums_df.iloc[0][['artist_names', 'album_name','ranking']]
                artist_names_b, album_name_b, ranking_b = albums_df.iloc[1][['artist_names', 'album_name', 'ranking']]
                category = albums_df.iloc[0]['category']
            
                album_name_a, _ = self.remove_parentheticals_and_drop_dash(album_name_a, RemoveWords.albums + RemoveWords.soundtracks)
                album_name_b, _ = self.remove_parentheticals_and_drop_dash(album_name_b, RemoveWords.albums + RemoveWords.soundtracks)
            
                rank_a = f'{Colors.YELLOW}#{ranking_a:.0f}' if not isna(ranking_a) else f'{Colors.GREY}unranked' #of {total}
                rank_b = f'{Colors.YELLOW}#{ranking_b:.0f}' if not isna(ranking_b) else f'{Colors.GREY}unranked' #of {total}

                print(f'Which {Colors.CYAN}{category}{Colors.END} release do you like better,\n'
                      f'\t{Colors.BLUE}[1]{Colors.END} {artist_names_a} - {album_name_a} '
                      f'(currently {rank_a}{Colors.END})\n\t\tor\n' #
                      f'\t{Colors.RED}[2]{Colors.END} {artist_names_b} - {album_name_b} '
                      f'(currently {rank_b}{Colors.END})\n\t\t?\n' #
                      f'\nPress {Colors.GREY}[Q]{Colors.END} to return to the main menu.\n')
                                
                key, loop = Stroker.get_keystroke(allowed_keys=['1', '2'], quit_key='Q')
                        
                if key in ['1', '2']:
                    print('...updating ranks...')
                    source_id_1, album_uri_1 = albums_df.loc[0][['source_id', 'album_uri']]
                    source_id_2, album_uri_2 = albums_df.loc[1][['source_id', 'album_uri']]
                    neon.update_album_comparisons(user_id, source_id_1, album_uri_1, source_id_2, album_uri_2, int(key))
                                       
            else:
                loop = False
               
    def rate_albums(self, neon, user_id):
        loop = True
        while loop:
            # get albums to rank
            print('...pulling up album to rate...\n')
            album_s = neon.get_album_to_rate(user_id, unrated=True)
            loop = self.rate_album(neon, user_id, album_s)
        
    def rate_album(self, neon, user_id, album_s):
        source_id, album_uri, album_name, artist_names, rating = album_s[['source_id', 'album_uri',
                                                                         'artist_names', 'album_name', 'rating']]
        if isna(rating):
            asis = f'{Colors.GREY}blank'
            current_rating = ''
        else:
            current_rating = str(int(rating))[-1]
            asis = f'as {Colors.GREEN}{current_rating}'

        album_name, _ = self.remove_parentheticals_and_drop_dash(album_name, RemoveWords.albums)
            
        ratings = [str(r + 1)[-1] for r in range(10)]
        print_ratings = '|'.join(f'{Colors.YELLOW if r == current_rating else ""}{r}{Colors.END}' for r in ratings)
        print(f'How would you rate {artist_names} - {album_name}? Choose {print_ratings} '
              f'or ENTER to leave {asis}{Colors.END}. Press [Q] to return to the main menu.')
        
        key, loop = Stroker.get_keystroke(allowed_keys=[str(r + 1) for r in range(10)] + ['ENTER'], quit_key='Q')            
        if loop and (key not in ['ENTER', current_rating]):
            rating = ratings.index(key) + 1
            print(f'...updating rating to {rating}...')
            neon.update_album_rating(user_id, source_id, album_uri, rating)

        return loop


class Turntable(Picker):
    def __init__(self):
        super().__init__()
        self.users = []
        self.record_stack = []
        self.needle = -1
        
    def play_music(self, neon, sonoser):
        
        user = self.select_user(message='listen to')   

        user_name = user.first_name + ' ' + user.last_name
        user_id = user.user_id
        print(f'Welcome {user_name}!')
        press_right = '[→] to play the next album'
        
        loop = True
        while loop:
            if self.needle >= 0:
                press_left = '[←] to go back to the previous album'
                press_up = '[↑] to replay this album'
                press_down = f'[↓] to {"pause" if sonoser.get_play_status() else "continue"} playing'
                allowed_keys = ['LEFT', 'RIGHT', 'UP', 'DOWN']
            else:
                press_left = press_up = press_down = None
                allowed_keys = ['RIGHT']
                
            press_choices = ', '.join(p for p in [press_left, press_up, press_down, press_right] if p)
            print(f'Press {press_choices} or [Q] to quit.')
            key, loop = Stroker.get_keystroke(allowed_keys=allowed_keys, quit_key='Q')
            if loop:
                match key:
                    case 'LEFT':
                        skip = -1
                    case 'RIGHT':
                        skip = 1
                    case 'UP':
                        skip = 0
                    
                if key in ['LEFT', 'RIGHT', 'UP']:
                    album_s = self.select_album(neon, user_id, skip)
                    self.play_album(sonoser, album_s)

                elif key == 'DOWN':
                    sonoser.change_play_status()
                
    def select_album(self, neon, user_id, skip=0):
        # get the next album to play
        self.needle += skip
        if self.needle >= len(self.record_stack):
            # add new album to the stack
            self.record_stack.append(neon.get_random_album(user_id))
        album_s = self.record_stack[self.needle]
        return album_s
        
    def play_album(self, sonoser, album_s):
        print(f"Playing {album_s['artist_names']} - {album_s['album_name']} from {album_s['service_name']}")
        sonoser.play_release(album_s['service_name'], album_s['track_list'])