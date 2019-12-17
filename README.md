# kye-mask-data

#### 介绍

主要用脱敏数据。

#### 软件架构

软件架构说明

#### MySQL 优化的配置

```sql
show variables like 'max_connections';
```

```bash
[mysqld]
port=3306
#socket=MySQL
skip-locking
set-variable = key_buffer=16K
set-variable = max_allowed_packet=1M
set-variable = thread_stack=64K
set-variable = table_cache=4
set-variable = sort_buffer=64K
set-variable = net_buffer_length=2K
max_connecionts=2000
```
