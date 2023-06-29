# IMPORT MODULES
import sys
sys.path.append('/spotify-data-pipeline/lib')
import spark_modules as lib_spark

# VARIABLES
PATH = "file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/Hot Hits Korea.json"

# BUILD SPARK SESSION
spark = lib_spark.build_spark_session()
print("---------------sesion build is done----------------------")

# READ JSON
dataframe = lib_spark.read_JSON(PATH)
playlist_name = dataframe.select('name').first()[0]
print("---------------dataframe is made----------------------")

# PARSE JSON DATAS
df_tracks = lib_spark.explode_dict(dataframe, "tracks")
df_items = lib_spark.explode_list(df_tracks, "items")
print("---------------df_items is made----------------------")

# SAVE AS PARQUET
# DIRECTORY NEEDS TO BE FIXED *********************
PARQUET_PATH = 'file:/Users/kimdohoon/git/spotify-data-pipeline/datas/parquets/items'
lib_spark.store_as_parquet(df_items, PARQUET_PATH)

# TEST
if __name__ == "__main__":
    pass
    # print(playlist_name)
    # dataframe.show()
    # df_tracks.show()
    # df_items.show()