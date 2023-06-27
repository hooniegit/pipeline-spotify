# spotify-data-pipeline
<img width="946" alt="스크린샷 2023-06-26 오후 4 54 42" src="https://github.com/hooniegit/spotify-data-pipeline/assets/130134750/4601e8cd-d880-46a0-9eae-bbd7b642d218">

### Goal
1. use spotify API(using spotipy) to create simple dataframe
2. practice query
3. change repeatable scripts into modules

# now status
<img width="1190" alt="스크린샷 2023-06-26 오후 2 31 50" src="https://github.com/hooniegit/spotify-data-pipeline/assets/130134750/49609609-a962-4b73-805c-2e1000da85c8">

### Process
1. start - basic
2. check.execute - basic
3. check.wishlist - extract list of playlists as a parameter
4. make.JSON.playlist - create temp JSON files by sending API requests
5. make.DONE - create DONE FLAG if the task is finished
6. run.spark - run local spark
7. spark.task.1 - simple parse job using modules & save as parquet
8. spark.task.2 - complex parse job & save as cashed parquet


# Tree Structure
```bash
.
.
├── README.md
├── dags
│   └── etl
│       └── neivekim76
│           ├── __pycache__
│           │   └── spotify-data-pipeline.cpython-37.pyc
│           └── spotify-data-pipeline.py
├── datas
│   ├── JSON
│   │   ├── artist_albums
│   │   │   └── IU.json
│   │   ├── artists
│   │   │   └── IU.json
│   │   └── playlists
│   │       ├── DONE
│   │       ├── Hot Hits Korea.json
│   │       ├── TrenChill K-R&B.json
│   │       ├── parquets
│   │       │   ├── items
│   │       │   │   ├── _SUCCESS
│   │       │   │   └── part-00000-5c9db5dd-cd47-4959-8043-3f7bb70fc85e-c000.snappy.parquet
│   │       │   └── table
│   │       │       ├── _SUCCESS
│   │       │       └── part-00000-a03c277f-9ee7-4a21-8830-085f5ea4c026-c000.snappy.parquet
│   │       └── 국내 R&B 메가 히트.json
│   └── wishlists
│       └── playlists.json
├── lib
│   ├── __pycache__
│   │   ├── spark_modules.cpython-37.pyc
│   │   └── spotipy_modules.cpython-37.pyc
│   ├── spark_modules.py
│   └── spotipy_modules.py
├── sh
│   ├── pyspark-submit.sh
│   └── run-spark.sh
└── src
    ├── API_requests
    │   └── neivekim76
    │       └── make_JSON_playlists.py
    └── spark
        └── neivekim76
            ├── TEST.py
            ├── spark_task_1.py
            └── spark_task_2.py
```
