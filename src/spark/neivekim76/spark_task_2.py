# IMPORT MODULES
import sys
sys.path.append('/Users/kimdohoon/git/spotify-data-pipeline/lib')
import spark_modules as lib_spark

# VARIABLES
PATH = "file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/Hot Hits Korea.json"

# BUILD SPARK SESSION
spark = lib_spark.build_spark_session()
print("---------------sesion build is done----------------------")

# READ PARQUET
PARQUET_PATH = 'file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/parquets/items/*'
dataframe = spark.read.parquet(PARQUET_PATH)
print("---------------dataframe is made----------------------")

# PARSE DATAFRAME
df_track = lib_spark.explode_dict(dataframe, "track")