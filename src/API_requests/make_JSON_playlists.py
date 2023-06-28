# IMPORT MODULES
import sys, json
sys.path.append('/spotify-data-pipeline/lib')
import spotipy_modules as lib_spotify

# VARIABLES
DIRECTORY_playlist = '/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists'
DIRECTORY_wishlist = '/Users/kimdohoon/git/spotify-data-pipeline/datas/wishlists/playlists.json'

# PLAYLISTS
with open(DIRECTORY_wishlist, 'r') as file:
    playlists_json = json.load(file)
playlists = playlists_json['playlists']

# LIB : MAKE PLAYLIST JSON DATAS
for playlist_id in playlists:
    lib_spotify.get_playlist(playlist_id, True, DIRECTORY_playlist)