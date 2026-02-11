s3://dario-kestra-nyc-data/
├── green/                                    # Green taxi staging (CSV)
│   └── year=2019/
│       └── month=01/
│           └── green_tripdata_2019-01.csv
│
├── yellow/                                   # Yellow taxi staging (CSV)
│   └── year=2019/
│       └── month=01/
│           └── yellow_tripdata_2019-01.csv
│
├── green_final/                              # Green taxi final (Parquet)
│   └── year=2019/
│       └── month=01/
│           └── [parquet files]
│
├── yellow_final/                             # Yellow taxi final (Parquet)
│   └── year=2019/
│       └── month=01/
│           └── [parquet files]
│
└── athena-results/                           # Athena query results
    └── [query output files]