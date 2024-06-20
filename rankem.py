''' Rank and rate albums '''

from setup import set_up_database
from music.users import Ranker, User

def rank_music(neon, user_id):
    ranker = Ranker()
    ranker.play(neon, user_id)

user_id = 1

if __name__ == '__main__':
    neon = set_up_database()
    user_s = neon.get_user(user_id)
    user = User(user_id, user_s['first_name'], user_s['last_name'])
    
    rank_music(neon, user)

    quit()