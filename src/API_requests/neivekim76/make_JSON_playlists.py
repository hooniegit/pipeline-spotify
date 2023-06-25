# IMPORT MODULES
import sys
sys.path.append('/Users/kimdohoon/git/spotify-data-pipeline/lib')
import spotipy_modules as lib

# VARIABLES
DIRECTORY_playlist = '/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists'
playlists = [
    "37i9dQZF1DWT9uTRZAYj0c",
    "37i9dQZF1DXbirtHQBuwCo",
    "37i9dQZF1DXe4qmDjDW0Ug"
]

# LIB : MAKE PLAYLIST JSON DATAS
for playlist_id in playlists:
    lib.get_playlist(playlist_id, True, DIRECTORY_playlist)
