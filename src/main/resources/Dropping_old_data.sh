--assuming hive on hdfs

create external table raw_zone(
col1 string,
col2 string,
col3 string

) partition by (load_date string,hour string)
   STORED AS parquet
   LOCATION
     'path-to_raw_data';

msck repair table raw_zone;

alter table raw_zone drop partitions (load_date<'dynamic_date',hour>=0);

drop table raw_zone

