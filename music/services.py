''' The various music services that this project can support '''

from .spotify import Spotter
from .soundcloud import Sounder
from .onedrive import Driver
from .musicbrainz import MusicBrainer
from .billboard import BBer
from .magazine import Critic
from .lastfm import FMer

UserServices = [Spotter, Sounder, Driver]

MUSIC_SERVICES = [{'name': 'Sonos', 'route': 'sonos', 'role': 'owner', 'required': True},
                  {'name': 'Spotify', 'route': 'spotify', 'role': 'guest'},
                  {'name': 'SoundCloud', 'route': 'soundcloud', 'role': 'guest'},
                  {'name': 'OneDrive', 'route': 'azure', 'role': 'owner'},
                  {'name': 'YouTube Music', 'route': 'youtube', 'role': 'guest'},
                  ]