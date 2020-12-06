CREATE EXTERNAL TABLE delta_table(
	-- fields of the table
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/path/to/table'
TBLPROPERTIES (
   "delta.compatibility.symlinkFormatManifest.enabled" = "true"
) ;