## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).


### source

copy from simple_key_distribution.cpp to jogasaki/src/jogasaki/dist/simple_key_distribution.cpp
and build jogasaki and tsurugidb

#### optional

If you want to check the status of the scan, please overwrite scan.cpp

cp scan jogasaki/src/jogasaki/executor/process/impl/ops/scan.cpp

and modify tsurugi.ini

```
[sql]
    // A number greater than the insert count.
    scan_block_size=1000000000
```

### modify tsurugi.ini

```
[sql]
    // 1,2,4,8,16,32,64,128,256,512
    scan_default_parallel=1
    // must be true
    dev_rtx_parallel_scan=true
    // must be simple
    dev_rtx_key_distribution=simple
```

### sbt setting

src/main/scala/Main.scala

Please modify src/main/scala/Main.scala to change the settings.

```
private val Connect = "ipc://tsurugi"
private val TableName = "test_table"
private val Columncount = 16000000
```

### sbt command

```
sbt compile
sbt run
```

### sql example

```
begin read only;
select count(*) from test_table;
commit;
```
