''' Play music '''

from setup import set_up_database, set_up_users
from music.hardware import Sonoser
from music.listeners import Turntable

def set_up_speakers():
    sonoser = Sonoser()
    sonoser.connect('mfranzonello@gmail.com')

    sonoser.get_households()
    sonoser.get_groups_and_players()
    sonoser.set_party_mode()
   
    return sonoser

def play_albums(neon, sonoser):
    users = set_up_users(neon)
    turntable = Turntable()
    turntable.add_users(users)
    turntable.play_music(neon, sonoser)
    
def main():
    neon = set_up_database()
    sonoser = set_up_speakers()
    play_albums(neon, sonoser)

if __name__ == '__main__':
    main()