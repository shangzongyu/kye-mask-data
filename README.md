# kye-mask-data

## 介绍

主要用脱敏数据。

## 软件架构

软件架构说明

## MySQL 优化的配置

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

## TODO

1. 使用 [[https://github.com/spf13/cobra]] 替代 flag
2. 有3个功能
  1. 生成测试数据
  2. 根据数据库生成配置文件
  3. 运行
     1. 直接更新数据库
     2. 生成配置文件

问题：

+ 可否不适用redis，如何保证数据脱敏的正确性，同一个数据对应的脱敏数据是一致的
+ 如何解决 ID 中间有数百万空的数据？这个需要详细解释

自己优化这个项目的过程，并且记录下来。

参数解析：刚开始使用 flag 解析
查询使用 Limit


POSIX-compliant flags ?

