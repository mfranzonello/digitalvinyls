''' Play music '''

from setup import set_up_database, set_up_users
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

def play_albums(neon, sonoser):
    users = set_up_users(neon)
    turntable = Turntable()
    turntable.add_users(users)
    turntable.play_music(neon, sonoser)
    
    # # print(sonoser.access_token)
    # # print(sonoser.household_id)
    # # print(sonoser.groups)
def man():
    neon = set_up_database()
    sonoser = set_up_speakers()
    play_albums(neon, sonoser)

if __name__ == '__main__':
    main()