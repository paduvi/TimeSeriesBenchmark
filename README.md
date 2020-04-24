# TimeSeriesBenchmark

## HBase

Mẫu file `settings.yaml`:

```yaml
HBASE_IP: ['HBASE_IP']
HBASE_PORT: 2181
HBASE_LOCATION: /hbase
HBASE_TABLE: timeseries_tbl
```

Trong trường hợp k có server HBase, có thể tạm thời chạy bằng Docker Standalone Hbase. 
Chi tiết xem trong file `docker/hbase/README.md` (Sử dụng lệnh `hostname` để lấy `HBASE_IP`)