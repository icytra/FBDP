# FBDP
金融大数据课程作业
## 作业5
作业5内容详见文件夹`211275024_许霁烨_作业5`，文件目录树如下：

```python
C:.
├─output
├─src
│  ├─main
│  │  ├─java
│  │  │  └─org
│  │  │      └─example
│  │  └─resources
│  └─test
│      └─java
└─target
    ├─classes
    │  └─org
    │      └─example
    ├─generated-sources
    │  └─annotations
    └─test-classes
```

- `output`文件夹内为程序输出结果
- `src/main/java/`文件夹内为代码及相关文件
- `hw5实验报告_许霁烨.pdf`为本次作业的实验报告
> update 2023-10-23
测试gitgraph提交

### 设计思路

**1. 目标：** 合并两个文件A.txt和B.txt，去除其中的重复内容，生成一个新的输出文件C。

> 这里提前把两个表格导出成了.txt格式，方便后续读入

**2. 输入和输出：**

- 输入文件A和B位于HDFS上的`/homework5/input`目录下。
- 输出文件C将保存在HDFS上的`/homework5/output`目录下。

**3. MapReduce工作流程：**

- **Mapper阶段：** 在 Map 阶段，每行文本内容都被作为键传递给 Reduce 阶段，并附带一个空值。
- **Reducer阶段：** 直接将输入中的 `key` 复制到输出数据的 `key` 上，并将一个空的 `Text` 作为输出值。这意味着在 Reduce 阶段，所有具有相同键的键值对都被合并在一起，但只保留了一个唯一的键。

**4. 配置和作业设置：**

- 配置Hadoop集群的信息，例如HDFS的地址和端口。
- 创建一个`Job`对象，命名为“Merge and duplicate removal”。
- 设置Mapper类和Reducer类，这里它们都是`Map`和`Reduce`。
- 指定输入路径为输入文件夹`/homework5/input`。
- 指定输出路径为输出文件夹`/homework5/output`。

### 运行成功的界面和运行截图：

见`hw5实验报告_许霁烨.pdf`中的运行结果。
