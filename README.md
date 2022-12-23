# flink-cdc实时同步mysql到doris工具

## 简介

这是一个基于flink-cdc的同步工具,将mysql数据同步到doris中.

一般情况下,我们启动一个cdc程序,书写过滤正则的时候,填写准确库名,表名则用*,表示我们这个cdc程序读取该库所有表.

这时,我们将面临如下问题:

- 该库新增了表,需要重启程序才能读取到该表
- 每次启动前需要提前手动在doris建立对应sink表
- mysql表结构变更,需要人工通知和维护doris上sink表的变更

此工具就是用于解决如上问题,做了如下操作:

- 对于新增表,自动在doris建表,并构建写入流
- 启动前整体检查是否存在对应的sink表,并自动创建
- 自动同步mysql列变化到doris中,并记录变动日志到doris表中做记录

## 后续改进

当前mysql中字段改名,doris中对应的表是新增一个同名字段.因为doris不支持字段改名.

随着新版本doris增加了字段改名功能,后续此处将同步修改

## 表结构同步变更功能说明

1. 支持同步的mysql ddl语句:
    - alter table `tableName` add column `column1` `type`
    - alter table `tableName` drop column `column1`
    - alter table `tableName` modify [column] `column1` `type`
    - alter table `tableName` change [column] `column1` `column2` `type`
2. 其他ddl语句处理:
    - `不在doris执行同步,但是将记录该ddl并插入到doris记录表中,表名:ops.cdcDdlRecord`
    - ops.cdcDdlRecord表结构如下:
    ```
    CREATE TABLE `cdcDdlRecord` (
    `time` datetime NULL COMMENT "日志时间",
    `host` varchar(60) NULL COMMENT "mysql host",
    `mysqlDb` varchar(60) NULL COMMENT "mysql database name",
    `mysqlTable` varchar(60) NULL COMMENT "mysql table name",
    `isFromCdc` varchar(10) NULL COMMENT "本条记录是否来自flinkCdc程序",
    `dorisTable` varchar(60) NULL COMMENT "对应的doris table name",
    `mysqlDdl` varchar(256) NULL COMMENT "经过处理后的mysql ddl",
    `dorisDdl` varchar(256) NULL COMMENT "程序转化得到doris ddl",
    `isExecute` varchar(10) NULL COMMENT "是否尝试在doris执行,1表示是",
    `isSucceed` varchar(10) NULL COMMENT "在doris中执行是否成功,1表示是",
    `columnsStatus` varchar(10) NULL COMMENT "sink列名列表状态:0=不需要更新列表,1=需要但是构建失败,2=需要且成功构建新的",
    `note` text NULL COMMENT "备注,后续人工检查可以添加此字段内容"
    ) ENGINE=OLAP
    DUPLICATE KEY(`time`, `host`, `mysqlDb`, `mysqlTable`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`time`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3",
    "in_memory" = "false",
    "storage_format" = "V2"
    );
   ```

3. `doris不支持列名修改`,当mysql进行列名修改的时候:
    - doris将新增一列,列信息如mysql修改后的列信息
    - doris将新增列紧挨着添加到旧列之后,旧数据在旧列,后续新数据写入新列中
    - 如:`ALTER table mysqlTable  change phone2 phone3 varchar(140)`,在doris中执行时候,将变为
      `ALTER table dorisTable  add column phone3 varchar(140) after phone2`
4. doris的列修改是异步执行的,可能失败(
   主要是列类型修改的时候,一般增加和删除列可以立即获取到结果,将在flink日志中打印相关信息),在doris执行如下命令可以查看:
   `SHOW ALTER TABLE COLUMN WHERE TableName = "dorisTable" ORDER BY CreateTime DESC LIMIT 1;`
5. 该功能是指程序运行期间的修改同步

---

## 同步创建新表功能说明

1. 此功能相当于同步 create table ddl,前提是该新表符合cdc设置的过滤正则
2. 将如与处理流程中的转化逻辑,将create table ddl转为doris ddl,同步在doris中建表
3. 同步构建指向新表的sink
4. 如建表时候需要自定义ddl,请先手动在doris建表,然后再在mysql执行相关建表操作.
   后续程序自动建表失败,但还是会构建相关sink,能正常传输数据到新表
5. 该功能是指程序运行期间,建立了符合表过滤正则的新表情况下,doris将同步建立表,并写入数据

## 使用建议

1. 对于大表,如需建立分区,请提前手动在doris建表,其余符合预处理转化逻辑的表,可以依赖自动建表功能
2. mysql建表字段类型推荐选用常见类型,因为doris数据类型较少,可能无法很好转换
3. 列类型变更同步:请谨慎核实列转换命令,尤其是有历史数据的情况下,历史数据将参与校验,可能出现历史数据无法转化为新类型.
   doris中列修改是异步执行的,涉及到历史数据转换的,可能无法立刻返回结果,后续需要手动查看结果.这种情况下不会报错,操作人员可能会误以为转换成功
4. doris varchar类型做修改的时候,长度只能比之前的大才会成功.
   即mysql中做varchar类型长度变更的时候,也要求新长度较大,才能成功同步该变更
5. sink并行度固定为1(不要更改),source并行度影响全量导入阶段,对后续实时读取binlog提升不大,推荐设置为1
6. sink并行度为1的原因:因为代码里为了实现表结构变更同步,要对streamLoad的实例进行替换,多个sink中只有一个sink能拿到变化的消息.
   如果sink多个并行度,表结构变更时候,会有streamLoad实例未替换,将漏字段.

## 如何使用

1. git clone到本地,用idea打开
2. 根据需求填写配置文件:mdw-mysql2dorisSyncTool.properties
3. idea调试:勾选带入'provided'依赖选项即可
4. 执行 mvn clean package,得到程序包(包含所有依赖的包才是我们需要的)
5. 到flink集群中,点击submit new job,提交包后,提交作业即可
