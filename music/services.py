''' The various music services that this project can support '''

from music.spotify import Spotter
from music.soundcloud import Sounder
from music.onedrive import Driver
from music.musicbrainz import MusicBrainer
from music.billboard import BBer
from music.magazine import Critic

UserServices = [Spotter, Sounder, Driver]