''' Listeners '''

import keyboard
from pandas import isna

from common.words import Texter, Colors
from library.stripping import RemoveWords

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
        
class Ranker(Texter):
    def __init__(self):
        super().__init__()
    
    def play(self, neon, user):
        user_name = f'{user.first_name} {user.last_name}'
        user_id = user.user_id
        
        print(f'Welcome {user_name}!')
        
        loop = True
        while loop:
            print(f'What would you like to do: {Colors.BLUE}[1]{Colors.END} rank albums, '
                  f'{Colors.RED}[2]{Colors.END} rate albums or {Colors.YELLOW}[3]{Colors.END} see results? '
                  f'Press {Colors.GREY}[Q]{Colors.END} to quit.')

            while True:
                event = keyboard.read_event()
                if event.event_type == keyboard.KEY_DOWN:
                    key = event.name.upper()
                    if key in ['1', '2', '3', 'Q']:
                        if key == 'Q':
                            loop = False
                        break

            if key in ['1', '2']:
                match key:
                    case '1':
                        self.rank_albums(neon, user_id)
                    case '2':
                        self.rate_albums(neon, user_id)
                
            if key in ['1', '2', '3']:
                self.get_summary(neon, user_id)

        print(f'See you later, {user_name}!')
       
    def get_summary(self, neon, user_id):
        albums_df = neon.get_album_summary(user_id, max_ranking=5)
        for category in albums_df['category'].unique():
            print(f'Top {Colors.CYAN}{category}{Colors.END} releases: ')
            print(albums_df.query('category == @category')[['ranking', 'artist_names', 'album_name']])
            print('Press any key to continue...')
            while True:
                event = keyboard.read_event()
                if event.event_type == keyboard.KEY_DOWN:
                    break
            
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
                                
                while True:
                    event = keyboard.read_event()
                    if event.event_type == keyboard.KEY_DOWN:
                        key = event.name.upper()
                        if key in ['1', '2', 'Q']:
                            if key == 'Q':
                                loop = False
                            break
                        
                if key in ['1', '2']:
                    print('...updating ranks...')
                    source_id_1, album_uri_1 = albums_df.loc[0][['source_id', 'album_uri']]
                    source_id_2, album_uri_2 = albums_df.loc[1][['source_id', 'album_uri']]
                    neon.update_album_comparisons(user_id, source_id_1, album_uri_1, source_id_2, album_uri_2, int(key))
                                       
            else:
                loop = False
            
    # # def rank_albums_2(self, neon, user_id):
    # #     loop = True
        
    # #     # get albums to rank
    # #     print('...pulling up albums to compare...\n')
    # #     albums_df, summary_df = neon.get_albums_to_compare(user_id)
    # #     if not albums_df.empty:
    # #         # user has albums to compare in this category
    # #         artist_name_a, album_name_a, ranking_a = albums_df.iloc[0][['artist_name', 'album_name', 'ranking']]
    # #         artist_name_b, album_name_b, ranking_b = albums_df.iloc[1][['artist_name', 'album_name', 'ranking']]
    # #         category = albums_df.iloc[0]['category']
    # #         total = summary_df.query('category == @category')['ranked'].sum()

    # #         rank_a = f'#{ranking_a:.0f} of {total}' if not isna(ranking_a) else 'unranked'
    # #         rank_b = f'#{ranking_b:.0f} of {total}' if not isna(ranking_b) else 'unranked'

    # #         print(f'Which {category} do you like better,\n'
    # #               f'\t{Colors.BLUE}[1]{Colors.END} {artist_name_a} - {album_name_a} \n\t\tor\n' #(currently {rank_a})
    # #               f'\t{Colors.RED}[2]{Colors.END} {artist_name_b} - {album_name_b} \n\t\t?\n' #(currently {rank_b})
    # #               f'\nPress {Colors.GREY}[Q]{Colors.END} to return to the main menu.\n')
                                
    # #         while True:
    # #             event = keyboard.read_event()
    # #             if event.event_type == keyboard.KEY_DOWN:
    # #                 key = event.name.upper()
    # #                 if key in ['1', '2', 'Q']:
    # #                     if key == 'Q':
    # #                         loop = False
    # #                     break
                        
    # #         if key in ['1', '2']:
    # #             print('...updating ranks...')
                    
    # #             ranking = None
    # #             if isna(ranking_b) and (key == '1'):
    # #                 # second is added to the list
    # #                 i = 2
    # #                 ranking = total + 1

    # #             elif isna(ranking_b) and (key == '2'):
    # #                 # second is moved ahead on the list
    # #                 i = 2
    # #                 ranking = ranking_a

    # #             elif isna(ranking_a) and (key == '1'):
    # #                 # first is moved ahead on the list
    # #                 i = 1
    # #                 ranking = ranking_b

    # #             elif isna(ranking_a) and (key == '2'):
    # #                 # first is added to the list
    # #                 i = 1
    # #                 ranking = total + 1

    # #             elif (ranking_a > ranking_b) and (key == '1'):
    # #                 # no movement
    # #                 pass

    # #             elif (ranking_a > ranking_b) and (key == '2'):
    # #                 # second is moved ahead on the list
    # #                 i = 2
    # #                 ranking = ranking_a
                        
    # #             elif (ranking_a < ranking_b) and (key == '1'):
    # #                 # first is moved ahead on the list
    # #                 i = 1
    # #                 ranking = ranking_b
                    
    # #             elif (ranking_a < ranking_b) and (key == '2'):
    # #                 # no movement
    # #                 pass
                    
    # #             if ranking:
    # #                 source_id, album_uri = albums_df.loc[i-1][['source_id', 'album_uri']]
    # #                 neon.update_album_rank(user_id, source_id, album_uri, category, ranking)
                    
    # #             for i in [0, 1]:
    # #                 album_s = albums_df.iloc[i]
    # #                 self.rate_album(neon, user_id, album_s)
                                       
    # #     else:
    # #         loop = False
                        
    # #     return loop
    
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
        
        while True:
            event = keyboard.read_event()
            if event.event_type == keyboard.KEY_DOWN:
                key = event.name.upper()
                if key in [str(r + 1) for r in range(10)] + ['ENTER'] + ['Q']:
                    break
                
        if (key not in ['ENTER', 'Q']) and (key != current_rating):
            rating = ratings.index(key) + 1
            print(f'...updating rating to {rating}...')
            neon.update_album_rating(user_id, source_id, album_uri, rating)

        return key != 'Q'