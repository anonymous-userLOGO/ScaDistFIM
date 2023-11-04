
## Frequent ItemSet Mining

使用fim命令运行spark和logo的FP-Growth算法，结果会输出到`[OutputHead]_[partitions]_[Spark|ScaDist].parquet`中
OutputHead可以通过options指定，如果不输入则会设为`FimExperiments/FimOutput-yyyy-MM-dd'T'HH-mm-ss`。
具体输出文件路径可以查看日志：`Write result [output-file]` 。

```
usage: fim [spark|scaDist] [OPTIONS] [source-file] [min-support] [partitions...]
Run FPGrowth
-help                    Show help.
-o,--output-head <arg>   result output file head.

examples:
  source-file = Sultan_Itemsets_64M.parquet
  min-support = 0.001
  partitions  = 5, 10 (rspRdd.getSub)

  run FPGrowth using spark mllib: fim spark Sultan_Itemsets_64M.parquet 0.001 5 10
  run FPGrowth using logo mllib scaDist: fim scaDist Sultan_Itemsets_64M.parquet 0.001 5 10
  
```

输出表格式为：

```
+---------------+-------+--------------------+
|          items|support|           frequency|
+---------------+-------+--------------------+
|          [118]|  39534|0.010315330688617598|
|          [182]|  34438|0.008985666976643216|
|     [182, 183]|  32716|0.008536357535509013|
|            [2]|  43876|0.011448258443208017|
|         [2, 3]|  37078|0.009674503750507496|
|         [2, 1]|  32944|0.008595847984160928|
|      [2, 1, 3]|  30760|0.008025992107600478|
|         [2, 0]|  32750| 0.00854522891820272|
|      [2, 0, 3]|  29350|0.007658090648832056|
|      [2, 0, 1]|  28856|0.007529194676752907|
|   [2, 0, 1, 3]|  27622|0.007207215669575436|
|         [2, 4]|  32478|0.008474257856653067|
|      [2, 4, 3]|  28878|0.007534934983201776|
|      [2, 4, 1]|  27904|0.007280795961329121|
|   [2, 4, 1, 3]|  26448|0.006900892043622154|
|      [2, 4, 0]|  27700|0.007227567665166...|
|   [2, 4, 0, 3]|  26744|0.006978125257661482|
|   [2, 4, 0, 1]|  26590|0.006937943112519399|
|[2, 4, 0, 1, 3]|  25912|0.006761037304686073|
|          [122]|  35570|0.009281031835739565|
+---------------+-------+--------------------+
```