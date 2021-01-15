# kye mask data

## 运行

```shell
./kye-mask -model run config.json
```

## 文件介绍

1. `config.json` : 配置文件

## 需要注意的问题

1. 数据库的主键名称必须为 ID，而且必须是 **整数类型**
2. 这个会直接修改源数据库，因此需要进行备份
3. 如果出现 Too Many Connection 修改配置 MySQL 配置文件

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
