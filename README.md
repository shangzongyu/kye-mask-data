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
max_connections=2000
```

## 文件介绍

1. `config.json` : 配置文件
2. `dbfile.txt` : 涉及到的数据库
3. `regex.txt` : 正则表达式

## 需要注意的问题

1. 数据库的主键名称必须为 ID，而且必须是 **整数类型**
2. 这个会直接修改源数据库，因此需要进行备份


```
./kye-mask -model generate-conf -conf config.json -generate-conf-db-file dbfile.txt -generate-conf-regexp-file regex.txt -generate-conf-db-username root -generate-conf-db-password SecOMP_2019 -generate-conf-db-host 10.121.20.42 -generate-conf-db-port 3306
```
