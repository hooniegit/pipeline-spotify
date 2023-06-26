# spotify-data-pipeline

# Tree Structure
```bash
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
│   │       │   └── items
│   │       │       ├── _SUCCESS
│   │       │       └── part-00000-5c9db5dd-cd47-4959-8043-3f7bb70fc85e-c000.snappy.parquet
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
            ├── spark_parse_playlist.ipynb
            ├── spark_task_1.py
            └── spark_task_2.py
```
