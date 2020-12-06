DROP TABLE IF EXISTS default.customer_delta;
DROP TABLE IF EXISTS default.customer_parquet;

CREATE EXTERNAL TABLE default.customer_delta(
	id int,
    firstName string,
    lastName string,
    age int,
    country string,
    deleted boolean
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://aphelip-ked-delta-lake/data/customer/_symlink_format_manifest/';


CREATE EXTERNAL TABLE default.customer_parquet(
	id int,
    firstName string,
    lastName string,
    age int,
    country string,
    deleted boolean
)
STORED AS PARQUET
LOCATION 's3://aphelip-ked-delta-lake/data/customer/';


SELECT * FROM default.customer_delta;

SELECT * FROM default.customer_parquet;
