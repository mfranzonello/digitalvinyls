''' Play music '''

from setup import set_up_database, set_up_user
from music.hardware import Sonoser, Turntable

def set_up_speakers():
    sonoser = Sonoser()
    sonoser.connect('mfranzonello@gmail.com')

    sonoser.get_households()
    sonoser.get_groups_and_players()
    sonoser.set_party_mode()

    # # print(sonoser.groups)
    # # print(sonoser.players)
    
    return sonoser

def play_album(neon, sonoser, user):
    turntable = Turntable()
    turntable.play_music(neon, sonoser, user)
    
    # # print(sonoser.access_token)
    # # print(sonoser.household_id)
    # # print(sonoser.groups)

user_id = 1

if __name__ == '__main__':
    neon = set_up_database()
    user = set_up_user(neon, user_id)
    sonoser = set_up_speakers()

    play_album(neon, sonoser, user)

    quit()
