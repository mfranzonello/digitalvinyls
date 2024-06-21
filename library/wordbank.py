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
