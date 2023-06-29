# IMPORT MODULES
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, explode
import sys
sys.path.append('/Users/kimdohoon/git/spotify-data-pipeline/lib')
import spark_modules

# BUILD SPARK SESSION
spark = SparkSession.builder.getOrCreate()

# READ PARQUET
# DIRECTORY NEEDS TO BE FIXED *********************
PARQUET_PATH = 'file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/parquets/items/*'
dataframe = spark.read.parquet(PARQUET_PATH)

# 1st Parse
dataframe = dataframe.withColumn("track_name", expr("track.name"))
df_specification = dataframe.select(
    "track.album",
    "track.artists",
    "track_name",
    "track.popularity"
)
df_specification.show()

# 2nd Parse
df_specification = df_specification.withColumn("album_type", expr("album.album_type"))
df_specification = df_specification.withColumn("album_images", expr("album.images"))
df_specification = df_specification.withColumn("album_name", expr("album.name"))
df_specification = df_specification.withColumn("album_artists", expr("album.artists"))
df_specification = df_specification.withColumn("album_artists", expr("album_artists.name"))
df_specification = df_specification.withColumn("artists_name", expr("artists.name"))
df_arranged = df_specification.select(
    "album_name",
    "album_artists",
    "album_type",
    "album_images",
    "track_name",
    "popularity",
    "artists_name"
)
df_arranged.show()

# album_artists, album_images, artists_name
df_arranged = df_arranged.withColumn('album_artist', explode(df_arranged['album_artists']).alias('album_artist'))
df_arranged = df_arranged.withColumn('artist_name', explode(df_arranged['artists_name']).alias('artist_name'))
df_arranged = df_arranged.select(
    "album_name",
    "album_artist",
    "album_type",
    "album_images",
    "track_name",
    "popularity",
    "artist_name"
)
df_arranged.show()


# PATH = "file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/parquets/table"
# spark_modules.store_as_parquet(df_arranged, PATH, True)
# print("---------------load is done----------------------")