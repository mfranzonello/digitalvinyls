''' Streaming music sources and libraries '''

from time import sleep

from pandas import DataFrame

from ..common.calling import Caller, Printer
from ..common.words import Texter
#from common.structure import
#from library.wordbank import RemoveWords
##from library.something import Something

class Service(Printer, Caller, Texter):
    name = None
    various_artist_uri = None
    
    max_artists_on_album = 4
    max_tracks_album_playlist = 30
    min_ep_tracks = 4
    min_album_tracks = 7
    min_album_duration = 20
    
    api_rate_limit = 1
    
    def __init__(self):
        Printer.__init__(self)
        Caller.__init__(self)
        Texter.__init__(self)
        
    def connect(self):
        pass

    def disconnect(self):
        pass
    
    def sleep(self, multiplier=1):
        sleep(multiplier/self.api_rate_limit)
        
    def announce(self, message):
        print(f'{message} from {self.name}')
        
    def combine_infos(self, info_0, info__0, existing_keys=[], i=None):
        if isinstance(info_0, list) and isinstance(info__0, list):
            eks = existing_keys if existing_keys else [[]]*len(info_0)
            for i_0, i__0, ek in zip(info_0, info__0, eks):
                i_0 = self.combine_infos(i_0, i__0, ek, i)

        else:
            for key in info__0.keys():
                if key in existing_keys:
                    info_0[key][i] = info__0[key]
                else:
                    info_0[key] = info_0.get(key, []) + info__0[key]

        return info_0

    def get_df_from_info(self, info, subset=[]):
        df = DataFrame(info)
        if subset:
            df.drop_duplicates(subset=subset, inplace=True)
        return df
        
class DSP(Service):
    def __init__(self):
        super().__init__()
        
    def add_various_artist_id(self, various_artist_uri):
        self.various_artist_uri = various_artist_uri
        
    def get_albums(self, **kwargs):
        return None, None, None

    def get_favorites(self, **kwargs):
        return None, None, None
    
    def get_playlists(self, **kwargs):
        return None, None, None
    
    def get_tracks_data(self, tracks_df):
        pass
    
    def get_artists_data(self, artists_df):
        pass
    
    def get_soundtracks_data(self, tracks_df):
        pass