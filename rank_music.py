''' Rank and rate albums '''

from .setup import set_up_database, set_up_users
from .music.listeners import Ranker

def rank_music(neon):
    users = set_up_users(neon)
    ranker = Ranker()
    ranker.add_users(users)
    ranker.play(neon)

def main():
    neon = set_up_database()
    rank_music(neon)

if __name__ == '__main__':
    main()