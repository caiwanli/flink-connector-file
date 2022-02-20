简单起见，connector 只支持按行读取指定目录的文件，在 SQL 语句中按如下方式使用 connector。
CREATE TABLE test (
  `line` STRING
) WITH (
  'connector' = 'file',
  'path' = 'file:///path/to/files'
);