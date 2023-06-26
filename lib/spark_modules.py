# IMPORT MODULES
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, expr
from pyspark.sql.types import BooleanType

# MODULE : build spark session
def build_spark_session():
    global spark
    spark = SparkSession.builder.getOrCreate()
    return spark

# MODULE : read JSON
def read_JSON(PATH):
    dataframe = spark.read.option("multiline", "true").json(PATH)
    return dataframe

# MODULE : explode list
# column name = COLUMN WHICH HAS LIST VALUE
def explode_list(dataframe, column_name):
    df_exploded = dataframe.select(explode(column_name).alias(column_name))
    df_parsed = df_exploded.select(f"{column_name}.*")
    return df_parsed

# MODULE : explode dict
# column name = COLUMN WHICH HAS DICT VALUE
def explode_dict(dataframe, column_name):
    columns = [field.name for field in dataframe.select(col(f"{column_name}.*")).schema.fields]
    expressions = []
    for i in columns:
        expressions.append(f"{column_name}."+i)
    df_parsed = dataframe
    for column, expression in zip(columns, expressions):
        df_parsed = df_parsed.withColumn(column, expr(expression))
    df_parsed = df_parsed.select([column for column in columns])
    return df_parsed

def explode_test(dataframe, column_name):
    columns = [field.name for field in dataframe.select(col(f"{column_name}.*")).schema.fields]
    expressions = []
    for i in columns:
        expressions.append(f"{column_name}."+i)
    df_parsed = dataframe
    for column, expression in zip(columns, expressions):
        if column == 'track' and df_parsed.schema[column].dataType == BooleanType():
            continue
        else:
            df_parsed = df_parsed.withColumn(column, expr(expression))
    df_parsed = df_parsed.select([column for column in columns])
    return df_parsed

# MODULE : filter boolean
def filter_boolean(dataframe):
    filtered_columns = [column.name for column in dataframe.schema if not isinstance(column.dataType, BooleanType)]
    df_filtered = dataframe.select(*filtered_columns)
    return df_filtered


def store_as_parquet(dataframe, PATH, cache=False):
    if cache == True:
        dataframe.cache()
        dataframe.coalesce(1).write.parquet(PATH)
    else:
        dataframe.write.parquet(PATH)
    print("dataframe stored")

# TEST
if __name__ == "__main__":
    # PARSING TEST
    PATH = "file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/Hot Hits Korea.json"
    build_spark_session()
    dataframe = read_JSON(PATH)
    dataframe.show()
    df_tracks = explode_dict(dataframe, "tracks")
    df_tracks.show()
    df_items = explode_list(df_tracks, "items")
    df_items.show()

    # STORING TEST
    PATH_TEST = "file:/Users/kimdohoon/git/spotify-data-pipeline/datas/JSON/playlists/parquets"
    store_as_parquet(df_items, PATH_TEST, True)