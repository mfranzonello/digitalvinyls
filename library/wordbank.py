''' Special words '''

from ..common.structure import read_yaml

_library_folder = 'library'
REMOVE_WORDS = read_yaml(_library_folder, 'wordbank')


class RemoveWords:
    albums = [{'position': 'start',
               'words': ['Remastered', 'Spotify Exclusive', '.*Anniversary Edition', 'Special.*Edition']},
              {'position': 'end',
               'words': ['Deluxe', 'Deluxe Edition', 'Deluxe Version', 'Remaster', 'Remastered', 'Standard Edition']}]
    soundtracks = [{'position': 'start',
                    'words': ['.*Motion Picture', 'Music From']},
                   {'position': 'end',
                    'words': ['Soundtrack']}]
    tracks = []
