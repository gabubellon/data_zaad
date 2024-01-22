CREATE TABLE `gb-data-zaad.spotify.data_hackers_episodes_gb` (
    id STRING,
    name STRING,
    description STRING,
    release_date DATE,
    duration_ms INT64,
    language STRING,
    explicit BOOL,
    type STRING
);
CREATE TABLE `gb-data-zaad.spotify.data_hackers_search` (
    id STRING,
    name STRING,
    description STRING,
    total_episodes INT64
);
CREATE TABLE `gb-data-zaad.spotify.data_hackers_episodes` (
    id STRING,
    name STRING,
    description STRING,
    release_date DATE,
    duration_ms INT64,
    language STRING,
    explicit BOOL,
    type STRING
);