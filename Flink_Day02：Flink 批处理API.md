---
stypora-copy-images-to: img
typora-root-url: ./
---



# Flink Day02：Flink 批处理 API

![1602831101602](/img/1602831101602.png)



## 01-[复习]-上次课程内容回顾 

> 上次主要讲解：Flink 安装部署和入门案例，具体如下思维导图所示。

![1614820086119](/img/1614820086119.png)

```
1、Flink on YARN 部署
	Session 会话模式
	Job 分离模式
	
2、入门案例：WordCount词频统计
	批处理，DataSet
	流计算，DataStream
	提交运行：命令行和UI界面
```





## 02-[掌握]-Flink 资源槽Slot 

> Flink中运行Task任务（SubTask）在Slot资源槽中：
> 								[Slot为子任务SubTask运行资源抽象，每个TaskManager运行时设置Slot个数。]()

```
官方建议：
	Slot资源槽个数  =  CPU Core核数
也就是说，
    分配给TaskManager多少CPU Core核数，可以等价为Slot个数
```

![1626485199097](/img/1626485199097.png)

> 每个TaskManager运行时设置内存大小：[TaskManager中内存==平均==划分给Slot]()

```
举例说明：
	假设TaskManager中分配内存为：4GB，Slot个数为4个，此时每个Slot分配内存就是 4GB / 4 = 1GB 内存
	
在Flink配置文件：FLINK_HOME/conf/flink-conf.yaml
```

![1629976542234](/img/1629976542234.png)

> 每个Slot中运行Task任务（SubTask子任务），以线程Thread方式运行。
>
> - 不同类型subTask任务，可以运行在同一个Slot中。
> - 相同类型subTask任务必须运行在不同Slot中。

![1630026217565](/img/1630026217565.png)





## 03-[了解]-第2天：课程内容提纲

> ​		主要讲解Flink 框架中进行`批处理API`使用：数据源Source、数据转换Transformation和数据终端Sink。

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/

```
1、Flink 环境准备
	开发环境构建：Maven Module模板，加入依赖
	词频统计：Scala和Java
	
2、数据源Source
	从哪里加载离线数据（有界数据流）
	1）、基于集合数据源
	2）、基于File数据源
	
3、数据终端Sink
	将数据保存到哪里？
	1）、打印控制台
	2）、保存文件中
	3）、自定义输出，比如输出到RDMBS数据库表
	
4、数据转换Transformation
	转换函数的使用
	1）、基础函数
	2）、关联函数JOIN
	3）、重分区函数
	
5、高级特性
	累加器
	广播变量
	分布式缓存
```

> 代码较多，多多练习，熟悉DataSet API使用

![1614820479006](/img/1614820479006.png)





## 04-[掌握]-第2天：开发环境准备 

​		Flink提供了多个层次的API供开发者使用，越往上抽象程度越高，使用起来越方便；越往下越底层，使用起来难度越大。

```
https://ci.apache.org/projects/flink/flink-docs-release-1.10/concepts/programming-model.html#levels-of-abstraction
```

![1614821491486](/img/1614821491486.png)



> 不同层次API，使用起来，不一样，下图展示不同层次API编程代码异同。

![1614821583030](/img/1614821583030.png)



> Flink 应用程序结构主要包含三部分：`Source/Transformation/Sink`，如下图所示：

![1614821606437](/img/1614821606437.png)

> Flink DataSet API编程基本步骤：
>
> - 1. 获取执行环境（`ExecutionEnvironment`）

![1614821629458](/img/1614821629458.png)

> - 2. 加载/创建初始数据集（`DataSet`），DataSet可以认为就是Spark中DataFrame或者Dataset

![1614821649479](/img/1614821649479.png)

> - 3. 对数据集进行各种转换（`Transformation`）操作，生成新的DataSet

![1614821685442](/img/1614821685442.png)

> 4. 指定计算的结果存储到哪个位置

![1614821722191](/img/1614821722191.png)



> 创建今天编写代码的`Maven Module`模块，添加相关依赖和创建包。

![1614821859664](/img/1614821859664.png)

> 查看模块依赖POM文件：

![1614821950997](/img/1614821950997.png)





## 05-[掌握]-Flink 词频统计之过滤及排序

> 编写批处理入门程序案例：词频统计WordCount，实现对`脏数据进行过滤`和`结果数据按照词频降序排序`。
>
> - 过滤数据：`filter`方法
> - 分区排序：`sortPartition`方法

```scala
package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 基于Flink引擎实现批处理词频统计WordCount：过滤filter、排序sort等操作
 */
public class _01WordCount {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source
		DataSource<String> inputDataSet = env.readTextFile("datas/wc.input");

		// 3. 数据转换-transformation
		AggregateOperator<Tuple2<String, Integer>> resultDataSet = inputDataSet
			// 3-1. 过滤脏数据
			.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String line) throws Exception {
					return null != line && line.trim().length() > 0;
				}
			})
			// 3-2. 单词分割
			.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public void flatMap(String line, Collector<String> out) throws Exception {
					for (String word : line.trim().split("\\s+")) {
						out.collect(word);
					}
				}
			})
			// 3-3. 转换二元组
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String word) throws Exception {
					return Tuple2.of(word, 1);
				}
			})
			// 3-4. 分组和求和
			.groupBy(0).sum(1);

		// 4. 数据终端-sink
		resultDataSet.print();

		System.out.println("================================================");

		// TODO: 对结果数据按照词频降序排序, sortPartition表示对每个分区数据排序
		SortPartitionOperator<Tuple2<String, Integer>> sortDataSet = resultDataSet.sortPartition(
			// 当数据类型为JavaBean时，指定属性名称，进行排序
			"f1",
			// 指定排序方式
			Order.DESCENDING
		).setParallelism(1);
		sortDataSet.printToErr();

        // TODO: 获取降序后，前3个元素 -> Top3
		GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> topDataSet = sortDataSet.first(3);
		topDataSet.printToErr();
        
		// 5. 触发执行-execute
	}

}
```





## 06-[掌握]-Flink 词频统计之Scala 版本 

> 使用`Scala`语言，实现离线批处理词频统计程序：WordCount。
>
> - 1、先过滤、再分割单词和转换
> - 2、最后分组聚合及分区数据排序

```scala
package cn.itcast.flink.start

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}

/**
 * 使用Scala语言实现词频统计：WordCount
 */
object _02FlinkWordCount {
	
	def main(args: Array[String]): Unit = {
		// 1. 执行环境-env
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(2)
		
		// 2. 数据源-source
		val inputDataSet: DataSet[String] = env.readTextFile("datas/wc.input")
		
		// 3. 数据转换-transformation
		val resultDataSet: DataSet[(String, Int)] = inputDataSet
			// 3-1. 过滤
			.filter(line => null != line && line.trim.length > 0)
			// 3-2. 分割单词
			.flatMap(line => line.trim.split("\\s+"))
			// 3-3. 转换二元组
			.map(word => (word, 1))
			// 3-4. 分组和求和
			.groupBy(0).sum(1)
			// 3-5. 降序排序, TODO: 并且设置Operator算子操作并行度为1，全局排序
			.sortPartition(1, Order.DESCENDING).setParallelism(1)
		
		// 4. 数据终端-sink
		resultDataSet.printToErr()
		
		// 5. 触发执行-execute
	}
	
}
```



## 07-[掌握]-数据源之基于Collection集合 

​			从本地集合（Java中集合）创建`DataSet`数据集，一般用于学习测试时模拟数据使用：

- 1、`env.fromElements`(可变参数)；
- 2、`env.fromColletion`(各种集合)；
- 3、`env.generateSequence`(开始,结束)；

![1630022329993](/img/1630022329993.png)

```java
package cn.itcast.flink.source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/**
 * DataSet API 批处理中数据源：基于集合Source
	 * 1.env.fromElements(可变参数);
	 * 2.env.fromColletion(各种集合);
	 * 3.env.generateSequence(开始,结束);
*/
public class BatchSourceCollectionDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 2. 数据源-source
		// 方式一：可变参数
		DataSource<String> dataSet01 = env.fromElements("spark", "flink", "hadoop");
		dataSet01.printToErr();

		// 方式二：Java集合对象
		DataSource<String> dataSet02 = env.fromCollection(Arrays.asList("spark", "flink", "hadoop"));
		dataSet02.printToErr();

		// 方式三：自动生成序列（整形）
		DataSource<Long> dataSet03 = env.generateSequence(1, 10);
		dataSet03.printToErr();
	}

}
```





## 08-[掌握]-数据源之基于File文件【压缩文件】

​			Flink DataSet 数据源也可以从文件中加载数据进行分析：

- 1、`env.readTextFile(本地文件/HDFS文件)`; //压缩文件也可以
- 2、`env.readCsvFile`[泛型]("本地文件/HDFS文件")
- 3、`env.readTextFile("目录").withParameters(parameters)`;



> 从文本文件加载数据时，可以是压缩文件，支持压缩格式如下图。

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#read-compressed-files

![1629978386384](/img/1629978386384.png)



> 案例演示代码如下所示：

```java
package cn.itcast.flink.source;

import lombok.Data;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * DataSet API 批处理中数据源：基于文件Source
	 * 1.env.readTextFile(本地文件/HDFS文件); //压缩文件也可以
	 * 2.env.readCsvFile[泛型]("本地文件/HDFS文件")
	 * 3.env.readTextFile("目录").withParameters(parameters);
 *  Configuration parameters = new Configuration();
 * 	 * parameters.setBoolean("recursive.file.enumeration", true);//设置是否递归读取目录
 */
public class BatchSourceFileDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// 方式一：读取文件，文本文件，可以是压缩
		DataSource<String> dataSet01 = env.readTextFile("datas/wordcount.tar.gz");
		dataSet01.printToErr();
	}
    
}
```



## 09-[掌握]-数据源之基于File文件【CSV文件】

​			加载读取CSV文件，很多种方式，官方案例代码：

![1629978008120](/img/1629978008120.png)

其中相关属性设置：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#configuring-csv-parsing

```java
@Data
public static class Rating{
    public Integer userId ;
    public Integer movieId ;
    public Double rating ;
    public Long timestamp ;
}

// 方式二：读取CSV文件
DataSource<Rating> dataSet02 = env
    .readCsvFile("datas/u.data")
    // 设置分隔符
    .fieldDelimiter("\t")
    // 执行POJO对象
    .pojoType(Rating.class, "userId", "movieId", "rating", "timestamp");
dataSet02.print();
```



## 10-[掌握]-数据源之基于File文件【递归遍历】

​		递归读取目录中文本文件数据，官方演示案例代码如下：

![1629977908352](/../03_%E7%AC%94%E8%AE%B0/img/1629977908352.png)

```scala
// 方式三：递归读取子目录中文件数据
Configuration parameters = new Configuration();
parameters.setBoolean("recursive.file.enumeration", true);
DataSource<String> dataSet03 = env
    .readTextFile("datas/subDatas")
    // 设置参数
    .withParameters(parameters);
dataSet03.printToErr();
```



## 11-[掌握]-数据终端之基于File文件【文本文件】

​		在Flink 批处理中，将数据分析以后，可以将数据保存到文件中，或打印控制台。

- 1、`ds.print` 直接输出到控制台
- 2、`ds.printToErr()` 直接输出到控制台,用红色
- 3、`ds.collect` 将分布式数据收集为本地集合
- 4、`ds.setParallelism(1).writeAsText("本地/HDFS的path", WriteMode.OVERWRITE)`
  - 在输出到path的时候，可以在前面设置并行度
    - [并行度>1，则path为目录]()
    - [并行度=1，则path为文件名]()

```java
package cn.itcast.flink.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;

/**
 * DataSet API 批处理中数据终端：基于文件Sink
	 * 1.ds.print 直接输出到控制台
	 * 2.ds.printToErr() 直接输出到控制台,用红色
	 * 3.ds.collect 将分布式数据收集为本地集合
	 * 4.ds.setParallelism(1).writeAsText("本地/HDFS的path",WriteMode.OVERWRITE)
 *
 * 注意: 在输出到path的时候，可以在前面设置并行度，如果
	 * 并行度>1，则path为目录
	 * 并行度=1，则path为文件名
 */
public class BatchSinkFileDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source
		DataSource<String> dataSet = env.readTextFile("datas/wordcount.data");

		// 3. 数据终端-sink
		// 方式一、方式二：控制台
		//dataSet.print();
		//dataSet.printToErr();

		// 方式三：转换为本地集合
		List<String> list = dataSet.collect();
		System.out.println(list);

		// 方式四：保存为文件
		dataSet.writeAsText("datas/sink-text.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// 5. 触发执行-execute
		env.execute(BatchSinkFileDemo.class.getSimpleName());
	}

}

```



## 12-[掌握]-数据终端之基于File文件【CSV文件】

> 将结果数据保存到CSV文件中，代码如下所示：
>
> [当将DataSet数据集保存为CSN文件时，要求数据类型为`元组Tuple`]()

```scala
		// step1. 数据类型为二元组
		MapOperator<String, Tuple2<String, Integer>> ds = dataSet.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String line) throws Exception {
				// 获取数据分区ID
				int index = getRuntimeContext().getIndexOfThisSubtask();
				// 返回二元组
				return Tuple2.of(line, index);
			}
		});

		// step2. 保存
		ds.writeAsCsv("datas/sink-csv.txt", "\n", "\t", FileSystem.WriteMode.OVERWRITE);

```





> 此外，可以将DataSet数据集保存到RDBMS数据库表中，调用方法：`output`。

![1630031160630](/img/1630031160630.png)

> 编程演示上述代码，伪代码：

```java
package cn.itcast.flink.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;

/**
 * DataSet API 批处理中数据终端，保存数据到RDBMs表中
 */
public class _05_03BatchSinkJdbcDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source
		DataSource<String> dataSet = env.fromElements("spark", "flink", "mapreduce");

		// 3. 数据终端-sink
		// 方式五：保存为CSV文件
		MapOperator<String, Row> mapDataSet = dataSet
			// 首相将数据转换为元组类型
			.map(new RichMapFunction<String, Row>() {
				@Override
				public Row map(String value) throws Exception {
					// 获取每条数据分区编号
					int index = getRuntimeContext().getIndexOfThisSubtask();
					// 构建二元组对象
//					return Tuple2.of(index, value);
					return Row.of(index, value) ;
				}
			});

		// TODO: 保存数据到数据库表中，使用JDBCOutputFormat，数据类型，必须是Row
		mapDataSet.output(
			JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("")
				.setDBUrl("")
				.setUsername("")
				.setPassword("")
				.setQuery("insert into ...........")
				.finish()
		);

		// 5. 触发执行-execute
		env.execute("BatchSinkCsvFileDemo");
	}

}
```



## 13-[理解]-数据转换之函数功能预览 

​		查看一下DataSet数据转换函数基本说明：

- https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/#dataset-transformations

- https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/batch/dataset_transformations.html#map

![1614826711294](/img/1614826711294.png)

> 通过案例，讲解上述函数基本使用，大多数函数/方法，在SparkCore中都是用过。

![1617677609829](/img/1617677609829.png)



讲解 基本函数时，[加载JSON格式日志数据文件，使用FastJson库将其解析为对应JavaBean对象]()。

- 1、JOSN格式行为日志数据

```JSON
{
	"browserType": "360浏览器",
	"categoryID": 2,
	"channelID": 3,
	"city": "昌平",
	"country": "china",
	"entryTime": 1577890860000,
	"leaveTime": 1577898060000,
	"network": "移动",
	"produceID": 11,
	"province": "北京",
	"source": "必应跳转",
	"userID": 18
}
```

[使用阿里巴巴开源提供FastJson库，解析JSON数据为JavaBean对象]()

- 2、封装JSON数据实体类`ClickLog`

```java
package cn.itcast.flink.transformation;

import lombok.Data;
@Data
public class ClickLog {
	//频道ID
	private long channelId;
	//产品的类别ID
	private long categoryId;

	//产品ID
	private long produceId;
	//用户的ID
	private long userId;
	//国家
	private String country;
	//省份
	private String province;
	//城市
	private String city;
	//网络方式
	private String network;
	//来源方式
	private String source;
	//浏览器类型
	private String browserType;
	//进入网站时间
	private Long entryTime;
	//离开网站时间
	private Long leaveTime;
}
```

> 首先，讲解DataSet函数中基本函数使用：
>
> [map、flatMap、filter、groupBy、sum、min、minBy、aggregate、reduce、reduceGroup、union、distinct]()





## 14-[掌握]-数据转换之map函数 

> map函数使用说明：

![1614826981456](/img/1614826981456.png)

```java
package cn.itcast.flink.transformation;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * Flink中批处理DataSet 转换函数基本使用
 */
public class BasicFunctionDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataSource<String> inputDataSet = env.readTextFile("datas/click.log");

		// 3. 数据转换-transformation
		// TODO: 函数一【map函数】，将JSON转换为JavaBean对象
		MapOperator<String, ClickLog> clickDataSet = inputDataSet.map(new MapFunction<String, ClickLog>() {
			@Override
			public ClickLog map(String line) throws Exception {
				// 使用FastJson库转换字符串为实体类对象
				return JSON.parseObject(line, ClickLog.class);
			}
		});
		clickDataSet.first(10).printToErr();


	}

}
```







## 15-[掌握]-数据转换之flatMap函数 

> `flatMap`：将集合中的每个元素变成一个或多个元素，并返回扁平化之后的结果，==flatMap = map + flattern==

![1614827326944](/img/1614827326944.png)

> 案例演示说明：依据`访问网站时间戳`转换为不同时间日期格式数据

```
Long类型日期时间：	1577890860000  
				|
				|进行格式擦欧洲哦
				|
String类型日期格式
		yyyy-MM-dd-HH
		yyyy-MM-dd
		yyyy-MM
```

![1614827370268](/img/1614827370268.png)

```java
		// TODO: 函数二【flatMap】，每条数据转换为日期时间格式
		FlatMapOperator<ClickLog, String> flatMapDataSet = clickDataSet.flatMap(new FlatMapFunction<ClickLog, String>() {
			@Override
			public void flatMap(ClickLog clickLog, Collector<String> out) throws Exception {
				// 获取日期时间字段值
				Long entryTime = clickLog.getEntryTime(); // 1577890860000 -> Long类型
				// 年-月-日-时： yyyy-MM-dd-HH
				String hour = DateFormatUtils.format(entryTime, "yyyy-MM-dd-HH");
				out.collect(hour);
				// 年-月-日： yyyy-MM-dd
				String day =  DateFormatUtils.format(entryTime, "yyyy-MM-dd");
				out.collect(day);
				// 年-月： yyyy-MM
				String month =  DateFormatUtils.format(entryTime, "yyyy-MM");
				out.collect(month);
			}
		});
		flatMapDataSet.first(10).printToErr();
```





## 16-[掌握]-数据转换之filter函数 

> `filter`：按照指定的条件对集合中的元素进行过滤，过滤出返回true/符合条件的元素

![1614827820790](/img/1614827820790.png)

> 需求：==过滤出clickLog中使用谷歌浏览器访问的日志==
>

```java
		// TODO: 函数三【filter函数】，过滤使用谷歌浏览器数据
		FilterOperator<ClickLog> filterDataSet = clickDataSet.filter(new RichFilterFunction<ClickLog>() {
			@Override
			public boolean filter(ClickLog clickLog) throws Exception {
				return "谷歌浏览器".equals(clickLog.getBrowserType());
			}
		});
		filterDataSet.first(10).printToErr();
```



## 17-[掌握]-数据转换之groupBy和sum函数 

> `groupBy`：对集合中的元素按照**指定的key**进行分组，通常与聚合函数一起使用，比如sum函数

![1614828041047](/img/1614828041047.png)



> `sum`：按照指定的字段对集合中的元素进行求和



==需求：对ClickLog按照浏览器类型分组，统计每个浏览器访问次数==

```java
		AggregateOperator<Tuple2<String, Integer>> sumDataSet = clickDataSet
			.map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(ClickLog clickLog) throws Exception {
					return Tuple2.of(clickLog.getBrowserType(), 1);
				}
			})
			// 按照浏览器类型分组
			.groupBy(0)
			.sum(1);
		sumDataSet.printToErr();
```

![1614828318536](/img/1614828318536.png)



## 18-[掌握]-数据转换之min和minBy函数 

​		在DataSet API中直接提供获取最大或最小值函数：`min与minBy，及max与maxBy`函数。

> - min`只会求出最小的那个字段`,其他的字段不管
>- minBy会`求出最小的那个字段和对应的其他的字段`

```scala
		/*
			(360浏览器,23)
			(qq浏览器,29)
			(火狐,24)
			(谷歌浏览器,24)
		 */
		//sumDataSet.printToErr();

		// TODO: 函数五【min和minBy】函数使用，求取最小值
		AggregateOperator<Tuple2<String, Integer>> minDataSet = sumDataSet.min(1);
		/*
			(谷歌浏览器,23)
		 */
		minDataSet.printToErr();

		ReduceOperator<Tuple2<String, Integer>> minByDataSet = sumDataSet.minBy(1);
		/*
			(360浏览器,23)
		 */
		minByDataSet.print();
```

![1626491906027](/img/1626491906027.png)





## 19-[掌握]-数据转换之aggregate函数 

> `aggregate`：按照指定的**聚合函数**和**字段**对集合中的元素进行聚合，如**[SUM,MIN,MAX]()**

![1614829778506](/img/1614829778506.png)

```java
		// TODO: 函数六【aggregate】函数，对数据进行聚合操作，需要指定聚合函数和字段
		AggregateOperator<Tuple2<String, Integer>> sumAggDataSet = sumDataSet.aggregate(Aggregations.SUM, 1);
		// (谷歌浏览器,100)
		sumAggDataSet.printToErr();

		AggregateOperator<Tuple2<String, Integer>> maxAggDataSet = sumDataSet.aggregate(Aggregations.MAX, 1);
		// (谷歌浏览器,29)
		maxAggDataSet.print();
```

> 使用aggregate函数时，**只关心指定字段的值，其他字段不关心**，任意的。
>





## 20-[掌握]-数据转换之reduce和reduceGroup函数 

首先，看一下`reduce`函数：“聚合”，需要==中间临时变量==。

```ini
list = [1, 2, 3, 4, 5]
1）、求和
	(tmp = 0, item) ->  sum
	tmp: 聚合时中间临时变量
		0 -> 1 -> 3 -> 6 -> 10 -> 15
	item：集合中每个元素
 
 
list = [11, 2, 8, 48, 25]    
2）、最大值
	(tmp = 0, item) -> max 
	tmp: 聚合中间临时变量，存储每次比较最大值
		0 -> 11 -> 11 -> 11 -> 48 -> 48
	item：集合中每个元素
```



> [`reduce`：对集合中的元素进行`聚合`]()

![1614830283976](/img/1614830283976.png)



> [reduceGroup：对集合中的元素`先进行预聚合``再合并结果`]()

![1614830350013](/img/1614830350013.png)



```java

		// TODO： 函数七【reduce和reduceGroup】使用
		UnsortedGrouping<Tuple2<String, Integer>> groupDataSet = clickDataSet
			.map(new MapFunction<ClickLog, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(ClickLog clickLog) throws Exception {
					return Tuple2.of(clickLog.getBrowserType(), 1);
				}
			})
			.groupBy(1);
		// reduce函数聚合
		ReduceOperator<Tuple2<String, Integer>> reduceDataSet = groupDataSet.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tmp,
			                                      Tuple2<String, Integer> item) throws Exception {
				return Tuple2.of(item.f0, tmp.f1 + item.f1);
			}
		});
		reduceDataSet.print();

		// reduceGroup函数，先聚合，在分组
		GroupReduceOperator<Tuple2<String, Integer>, String> groupReduceDataSet = groupDataSet.reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, String>() {
			@Override
			public void reduce(Iterable<Tuple2<String, Integer>> values,
			                   Collector<String> out) throws Exception {
				// 定义变量
				String key = "" ;
				int count = 0 ;
				for(Tuple2<String, Integer> item: values){
					key = item.f0 ;
					count += item.f1;
				}
				// 输出
				out.collect(key + ", " + count);
			}
		});
		groupReduceDataSet.printToErr();
```



## 21-[掌握]-数据转换之union函数 

> `union`：将两个集合进行合并但不会去重，[集合中数据类型一致]()

![1614830887256](/img/1614830887256.png)

![1614830902494](/img/1614830902494.png)

```java
		// TODO: 函数八【union函数】，合并数据类型相同2个数据集
		MapOperator<String, ClickLog> dataSet01 = env
			.readTextFile("datas/input/click1.log")
			.map(new MapFunction<String, ClickLog>() {
				@Override
				public ClickLog map(String line) throws Exception {
					// 使用FastJson库转换字符串为实体类对象
					return JSON.parseObject(line, ClickLog.class);
				}
			});
		MapOperator<String, ClickLog> dataSet02 = env
			.readTextFile("datas/input/click2.log")
			.map(new MapFunction<String, ClickLog>() {
				@Override
				public ClickLog map(String line) throws Exception {
					// 使用FastJson库转换字符串为实体类对象
					return JSON.parseObject(line, ClickLog.class);
				}
			});
		// 使用union函数，进行合并
		UnionOperator<ClickLog> unionDataSet = dataSet01.union(dataSet02);
		System.out.println("DataSet01 条目数：" + dataSet01.count());
		System.out.println("DataSet02 条目数：" + dataSet02.count());
		System.out.println("unionDataSet 条目数：" + unionDataSet.count());
```

> SQL语句中`union`和`union all`区别

![1630036167187](/img/1630036167187.png)





## 22-[掌握]-数据转换之distinct函数 

> `distinct`：对集合中的元素进行去重

![1614831182536](/img/1614831182536.png)

```java
		// TODO: 函数九【distinct函数】对数据进行去重操作
		DistinctOperator<ClickLog> distinctDataSet = clickDataSet.distinct("browserType");
		distinctDataSet.printToErr();
```

![1630036393047](/img/1630036393047.png)

> - 1）、distinct函数，**指定参数值**，表示按照字段值进行去重
> - 2）、如果不指定的话，整条数据各个字段值相同时，进行去重





## 23-[掌握]-数据转换之join函数 

> `join`：将两个集合（DataSet）按照`指定的条件`进行连接

![1630045510951](/img/1630045510951.png)



需求：==将学生成绩数据`score.csv`和学科数据`subject.csv`进行关联,得到学生对应学科的成绩==

```
成绩数据：score.csv
    1,张三,1,98
    2,张三,2,77.5
    3,张三,3,89
    4,张三,4,65
    5,张三,5,78
    6,张三,6,70
    7,李四,1,78
    8,李四,2,58
    9,李四,3,65
    10,李四,4,78
    11,李四,5,70
    12,李四,6,78
    13,王五,1,70
    14,王五,2,78
    15,王五,3,58
    16,王五,4,65
    17,王五,5,78
    18,王五,6,98
    19,赵六,1,77.5
    20,赵六,2,89
    21,赵六,3,65
    22,赵六,4,78
    23,赵六,5,70
    24,赵六,6,78
    25,小七,1,78
    26,小七,2,70
    27,小七,3,78
    28,小七,4,58
    29,小七,5,65
    30,小七,6,78


学科数据：subject.csv
	1,语文
    2,数学
    3,英语
    4,物理
    5,化学
    6,生物
```

![1614840033242](/img/1614840033242.png)

```java
package cn.itcast.flink.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;

/**
 * Flink 框架中批处理实现两个数据集关联分析：JOIN
 */
public class BatchJoinDemo {
	@Data
	public static class Score{
		private Integer id;
		private String stuName;
		private Integer subId;
		private Double score;
	}
	@Data
	public static class Subject {
		private Integer id;
		private String name;
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// 学生成绩数据score.csv: datas/score.csv
		DataSource<Score> scoreDataSet = env
			.readCsvFile("datas/score.csv")
			.fieldDelimiter(",")
			.pojoType(Score.class, "id", "stuName", "subId", "score");

		// 学科数据subject.csv: datas/subject.csv
		DataSource<Subject> subjectDataSet = env
			.readCsvFile("datas/subject.csv")
			.fieldDelimiter(",")
			.pojoType(Subject.class, "id", "name");

		// 3. 数据转换-transformation
		// TODO: 等值JOIN
		// SQL: SELECT ... FROM score sc JOIN subject sb ON sc.subId = sb.id ;
		JoinOperator.EquiJoin<Score, Subject, String> joinDataSet = scoreDataSet
			.join(subjectDataSet)
			.where("subId").equalTo("id")
			.with(new JoinFunction<Score, Subject, String>() {
				@Override
				public String join(Score score, Subject subject) throws Exception {
					return score.id + ", " + score.stuName + ", " + subject.name + ", " + score.score;
				}
			});
		joinDataSet.printToErr();


	}

}
```





## 24-[理解]-数据转换之左外连接LeftJoin 

​		2个数据集DataSet之间进行等值关联JOIN，在实际项目中，更多的就是外连接（OuterJoin）：

> - 1）、左外连接：`LeftOuterJoin`，以左表为准，
>  - leftOuterJoin: 左外连接, 左边集合的元素全部留下,右边的满足条件的元素留下

![1614840799654](/img/1614840799654.png)

​		编写代码，实现DataSet数据集左外连接，演示代码如下：

```JAva
package cn.itcast.flink.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Flink 框架中批处理实现两个数据集关联分析：JOIN
 */
public class BatchJoinDemo {
	@Data
	public static class Score{
		private Integer id;
		private String stuName;
		private Integer subId;
		private Double score;
	}
	@Data
	public static class Subject {
		private Integer id;
		private String name;
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// TODO： 外连接函数使用
		DataSource<Tuple2<Integer, String>> userDataSet = env.fromElements(
			Tuple2.of(1, "tom"), Tuple2.of(2, "jack"), Tuple2.of(3, "rose")
		);
		DataSource<Tuple2<Integer, String>> cityDataSet = env.fromElements(
			Tuple2.of(1, "北京"), Tuple2.of(2, "上海"), Tuple2.of(4, "广州")
		);
		// TODO: 左外连接leftJoin
		JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> leftJoinDataSet = userDataSet
			.leftOuterJoin(cityDataSet)
			.where(0).equalTo(0)
			.with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
				@Override
				public String join(Tuple2<Integer, String> user,
				                   Tuple2<Integer, String> city) throws Exception {
					if (null == city) {
						return user.f0 + ", " + user.f1 + ", " + "未知";
					}else {
						return user.f0 + ", " + user.f1 + ", " + city.f1;
					}
				}
			});
		leftJoinDataSet.printToErr();
	}

}
```





## 25-[理解]-数据转换之右外连接RightJoin 

> - 2）、右外连接：`RightOuterJoin`，以右表为准
>   - rightOuterJoin: 右外连接, 右边集合的元素全部留下,左边的满足条件的元素留下

![1614840833150](/../03_%E7%AC%94%E8%AE%B0/img/1614840833150.png)

​				编写代码，实现DataSet数据集右外连接，演示代码如下：

```java
// TODO: 右外连接rightJoin
JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> rightJoinDataSet = userDataSet
	.rightOuterJoin(cityDataSet)
    .where(0).equalTo(0)
    .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
        @Override
        public String join(Tuple2<Integer, String> user,
                           Tuple2<Integer, String> city) throws Exception {
            if (null == user) {
                return city.f0 + ", " + "未知" + ", " + city.f1 ;
            }else {
                return city.f0 + ", " + user.f1 + ", " + city.f1;
            }
        }
    });
rightJoinDataSet.print();
```



## 26-[理解]-数据转换之全外连接FullOuterJoin 

> - 3）、全外连接：`FullOuterJoin = LeftOuterJoin + RightOuterJoin + DINSTINCT`
>   - fullOuterJoin: 全外连接, 左右集合中的元素全部留下

![1614840853334](/../03_%E7%AC%94%E8%AE%B0/img/1614840853334.png)

​		编写代码，实现DataSet数据集全外连接，演示代码如下：

```java
// TODO: 全外连接
JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, String> fullJoinDataSet = userDataSet
	.fullOuterJoin(cityDataSet)
    .where(0).equalTo(0)
    .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
        @Override
        public String join(Tuple2<Integer, String> user,
                           Tuple2<Integer, String> city) throws Exception {
            if (null == city) {
                return user.f0 + ", " + user.f1 + ", " + "未知";
            } else if (null == user) {
                return city.f0 + ", " + "未知" + ", " + city.f1 ;
            }else {
                return city.f0 + ", " + user.f1 + ", " + city.f1;
            }
        }
    });
fullJoinDataSet.printToErr();
```



## 27-[掌握]-数据转换之rebalance函数 

> `rebalance` 重平衡分区：类似于Spark中的repartition，但是功能更强大，可以**直接解决数据倾斜**

![1614841611187](/img/1614841611187.png)



> ​			所以在实际的工作中，出现这种情况比较好的解决方案就是`rebalance`(内部使用`round robin`方法将数据均匀打散)。[采用方法：轮流机制，公平公正]()

![1614841747025](/img/1614841747025.png)



案例演示代码如下：

```scala
package cn.itcast.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * Flink 批处理DataSet API中分区函数
 *      TODO: rebalance、partitionBy*
 */
public class BatchRepartitionDemo {

	public static void main(String[] args) throws Exception {

		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		// 2. 数据源-source
		DataSource<Long> dataset = env.generateSequence(1, 30);
		dataset.map(new RichMapFunction<Long, String>() {
			@Override
			public String map(Long value) throws Exception {
				return getRuntimeContext().getIndexOfThisSubtask() + ": " + value;
			}
		}).printToErr();

		// TODO: 将各个分区数据不均衡，过滤
		FilterOperator<Long> filterDataSet = dataset.filter(new FilterFunction<Long>() {
			@Override
			public boolean filter(Long value) throws Exception {
				return value > 5 && value < 26;
			}
		});
		filterDataSet.map(new RichMapFunction<Long, String>() {
			@Override
			public String map(Long value) throws Exception {
				return getRuntimeContext().getIndexOfThisSubtask() + ": " + value;
			}
		}).print();

		// TODO: 使用rebalance函数，对DataSet数据集数据进行重平衡
		filterDataSet
			.rebalance()
			.map(new RichMapFunction<Long, String>() {
				@Override
				public String map(Long value) throws Exception {
					return getRuntimeContext().getIndexOfThisSubtask() + ": " + value;
				}
			})
			.printToErr();
	}

}

```

> 将各个DataSet数据集数据打印出来，如下截图所示：

![1626504282228](/img/1626504282228.png)





## 28-[理解]-数据转换之其他重分区函数 

> 在分区中最常见的方式：`hash哈希分区`及`range范围分区`。

![1629010156001](/img/1629010156001.png)

> ​		Flink批处理中，还提供其他分区函数，比如按照某个字段进行哈希分区、范围分区及自定义分区规则。

![1614842291617](/img/1614842291617.png)

```java
		// ====================================================================
		// TODO: 其他一些分区函数，针对数据类型为Tuple元组
		List<Tuple3<Integer,Long,String>> list = new ArrayList<>();
		list.add(Tuple3.of(1, 1L, "Hello"));
		list.add(Tuple3.of(2, 2L, "Hello"));
		list.add(Tuple3.of(3, 2L, "Hello"));
		list.add(Tuple3.of(4, 3L, "Hello"));
		list.add(Tuple3.of(5, 3L, "Hello"));
		list.add(Tuple3.of(6, 3L, "hehe"));
		list.add(Tuple3.of(7, 4L, "hehe"));
		list.add(Tuple3.of(8, 4L, "hehe"));
		list.add(Tuple3.of(9, 4L, "hehe"));
		list.add(Tuple3.of(10, 4L, "hehe"));
		list.add(Tuple3.of(11, 5L, "hehe"));
		list.add(Tuple3.of(12, 5L, "hehe"));
		list.add(Tuple3.of(13, 5L, "hehe"));
		list.add(Tuple3.of(14, 5L, "hehe"));
		list.add(Tuple3.of(15, 5L, "hehe"));
		list.add(Tuple3.of(16, 6L, "hehe"));
		list.add(Tuple3.of(17, 6L, "hehe"));
		list.add(Tuple3.of(18, 6L, "hehe"));
		list.add(Tuple3.of(19, 6L, "hehe"));
		list.add(Tuple3.of(20, 6L, "hehe"));
		list.add(Tuple3.of(21, 6L, "hehe"));
		// 将数据打乱，进行洗牌
		Collections.shuffle(list);

		DataSource<Tuple3<Integer, Long, String>> tupleDataSet = env.fromCollection(list);
		env.setParallelism(2);

		// TODO: a. 指定字段，按照hash进行分区
		tupleDataSet
			.partitionByHash(2)
			.map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
				@Override
				public String map(Tuple3<Integer, Long, String> value) throws Exception {
					int index = getRuntimeContext().getIndexOfThisSubtask();
					return index + ": " + value.toString();
				}
			})
			.printToErr();
		
		// TODO: b. 指定字段，按照Range范围进行分区
		tupleDataSet
			.partitionByRange(0)
			.map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
				@Override
				public String map(Tuple3<Integer, Long, String> value) throws Exception {
					int index = getRuntimeContext().getIndexOfThisSubtask();
					return index + ": " + value.toString();
				}
			})
			.printToErr();

		// TODO: c. 自定义分区规则
		tupleDataSet
			.partitionCustom(new Partitioner<Integer>() {
				                 @Override
				                 public int partition(Integer key, int numPartitions) {
					                 return key % 2;  // 奇数，偶数 划分
				                 }
			                 }, 0
			)
			.map(new RichMapFunction<Tuple3<Integer, Long, String>, String>() {
				@Override
				public String map(Tuple3<Integer, Long, String> value) throws Exception {
					int index = getRuntimeContext().getIndexOfThisSubtask();
					return index + ": " + value.toString();
				}
			})
			.printToErr();
```





## 29-[了解]-高级特性之累加器Accumulator 

​				[Flink中的累加器，与Mapreduce counter的应用场景类似，可以很好地观察task在运行期间的数据变化，如在Flink job任务中的算子函数中操作累加器，在任务执行结束之后才能获得累加器的最终结果。]()

> Flink有以下内置累加器，每个累加器都实现了`Accumulator`接口：

![1614843761829](/img/1614843761829.png)

> 使用累加器进行累加统计操作时，步骤如下：

```ini
1.创建累加器
	private IntCounter numLines = new IntCounter();
2.注册累加器
	getRuntimeContext().addAccumulator("num-lines", this.numLines);
3.使用累加器
	this.numLines.add(1);
4.获取累加器的结果
	myJobExecutionResult.getAccumulatorResult("num-lines")
```

> ==编写程序==：对数据源读取的数据进行计数Counter，最终输出所出来的数据条目数。

```java
package cn.itcast.flink.other;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;


/**
 * 演示Flink中累加器Accumulator使用，统计处理的条目数
 */
public class BatchAccumulatorDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataSource<String> dataset = env.readTextFile("datas/click.log");

		// 3. 数据转换-transformation
		// TODO: 此处，使用map函数，不做任何处理，仅仅为了使用累加器
		MapOperator<String, String> ds = dataset.map(new RichMapFunction<String, String>() {
			// TODO: step1. 定义累加器
			private LongCounter counter = new LongCounter();

			@Override
			public void open(Configuration parameters) throws Exception {
				// TODO: step2. 注册累加器
				getRuntimeContext().addAccumulator("counter", counter);
			}

			@Override
			public String map(String value) throws Exception {
				// TODO： step3. 使用累加器，进行计数操作
				counter.add(1L);
				return value;
			}
		});

		// 4. 数据终端-sink
		ds.writeAsText("datas/sk-log.txt");

		// 5. 触发执行-execute
		JobExecutionResult jobResult = env.execute(BatchAccumulatorDemo.class.getSimpleName());
		// TODO: step4. 获取累加器的值
		Object counterValue = jobResult.getAccumulatorResult("counter");
		System.out.println("Counter: " + counterValue);
	}

}

```





## 30-[掌握]-高级特性之广播变量 

> ​		Flink支持广播，可以==将数据广播到TaskManager==上就可以==供TaskManager中的SubTask/task==去使用，`数据存储到内存`中。

![1614844524079](/img/1614844524079.png)

- 可以理解广播就是一个公共的共享变量
- 将一个数据集广播后，不同的Task都可以在节点上获取到
- 每个节点只存一份
- 如果不使用广播，每一个Task都会拷贝一份数据集，造成`内存资源浪费`

> - 1）、广播变量是要把dataset广播到内存中，所以广播的数据量不能太大，否则会出现`OOM`
>    - 小表数据
> - 2）、广播变量的值不可修改，这样才能确保每个节点获取到的值都是一致的

​		使用广播变量步骤：

```
1：广播数据
	.withBroadcastSet(DataSet, "name");
2：获取广播的数据
	Collection<> broadcastSet = getRuntimeContext().getBroadcastVariable("name");
3：使用广播数据
```



​		将`studentDS(学号,姓名)`集合广播出去(广播到各个TaskManager内存中)，然后使用`scoreDS(学号,学科,成绩)`和广播数据(学号,姓名)进行关联，得到这样格式的数据：`(姓名,学科,成绩)`

![1626506982406](/img/1626506982406.png)

​				编写代码，完成小数据集DataSet广播，与大数据集进行关联操作，代码如下所示：

```scala
package cn.itcast.flink.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 批处理中广播变量：将小数据集广播至TaskManager内存中，便于使用
 */
public class BatchBroadcastDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source：从本地集合构建2个DataSet
		DataSource<Tuple2<Integer, String>> studentDataSet = env.fromCollection(
			Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五"))
		);
		DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
			Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英语", 86))
		);

		// 3. 数据转换-transform：使用map函数，定义加强映射函数RichMapFunction，使用广播变量值
		/*
			(1, "语文", 50) -> "张三" , "语文", 50
			(2, "数学", 70) -> "李四", "数学", 70
			(3, "英语", 86) -> "王五", "英语", 86
		*/
		MapOperator<Tuple3<Integer, String, Integer>, String> dataSet = scoreDataSet
			.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {

				// 定义Map集合，存储小表的数据：学生数据 -> key: id, value: name
				private Map<Integer, String> stuMap = new HashMap<>() ;

				@Override
				public void open(Configuration parameters) throws Exception {
					// TODO: step2. 调用map方法之前，从TaskManager中获取广播的数据
					List<Tuple2<Integer, String>> list = getRuntimeContext().getBroadcastVariable("students");

					// TODO: step3. 使用广播变量的值
					for (Tuple2<Integer, String> item : list) {
						stuMap.put(item.f0, item.f1) ;
					}
				}

				@Override
				public String map(Tuple3<Integer, String, Integer> score) throws Exception {
					String name = stuMap.getOrDefault(score.f0, "未知") ;
					return name + ", " + score.f1 + ", " + score.f2;
				}
			})
			// TODO：step1. 将小表数据广播出去
			.withBroadcastSet(studentDataSet, "students");

		// 4. 数据终端-sink
		dataSet.printToErr();
	}

}
```



## 31-[掌握]-高级特性之分布式缓存 

Flink提供了一个类似于Hadoop的分布式缓存，让并行运行实例的函数可以在本地访问。

- 广播变量：使用数据为数据集DataSet，将其广播到TaskManager内存中，被Task使用；
- 分布式缓存：缓存数据文件数据，数据放在文件中；

[广播变量是将`变量（DataSet）`分发到各个TaskManager节点的内存上，分布式缓存是将`文件缓存`到各个TaskManager节点上；]()



> 编码步骤：

![1614845982591](/img/1614845982591.png)

```java
package cn.itcast.flink.other;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink 批处理中分布式缓存：将小文件数据进行缓存
 */
public class BatchDistributedCacheDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		// TODO: step1. 将小文件数据进行缓存
		env.registerCachedFile("datas/distribute_cache_student", "student_cache");

		// 2. 数据源-source：从本地集合构建2个DataSet
		DataSource<Tuple3<Integer, String, Integer>> scoreDataSet = env.fromCollection(
			Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英语", 86))
		);

		// 3. 数据转换-transform：使用map函数，定义加强映射函数RichMapFunction，使用广播变量值
		/*
			(1, "语文", 50) -> "张三" , "语文", 50
			(2, "数学", 70) -> "李四", "数学", 70
			(3, "英语", 86) -> "王五", "英语", 86
		*/
		MapOperator<Tuple3<Integer, String, Integer>, String> dataset = scoreDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, String>() {

			// 定义Map集合，存储小表的数据：学生数据 -> key: id, value: name
			private Map<Integer, String> stuMap = new HashMap<>() ;

			@Override
			public void open(Configuration parameters) throws Exception {
				// TODO: step2. 从分布式缓存中获取文件
				File file = getRuntimeContext().getDistributedCache().getFile("student_cache");

				// TODO: step3. 读取缓存文件内容，解析存储到Map集合
				List<String> list = FileUtils.readLines(file);  // 一行一行读取文件数据，放到列表中List
				for(String item: list){
					String[] split = item.trim().split(",");
					stuMap.put(Integer.valueOf(split[0]), split[1]) ;
				}
			}

			@Override
			public String map(Tuple3<Integer, String, Integer> score) throws Exception {
				String name = stuMap.getOrDefault(score.f0, "未知") ;
				return name + ", " + score.f1 + ", " + score.f2;
			}
		});

		// 4. 数据终端-sink
		dataset.printToErr();
	}
}
```





## [附录]-创建Maven模块

> Maven 工程结构

![1602937986744](/img/1602937986744.png)



> Maven 工程POM文件中内容（依赖包）：		
>

```XML
    <repositories>
        <repository>
            <id>apache.snapshots</id>
            <name>Apache Development Snapshot Repository</name>
            <url>https://repository.apache.org/content/repositories/snapshots/</url>
            <releases>
                <enabled>false</enabled>
            </releases>
            <snapshots>
                <enabled>true</enabled>
            </snapshots>
        </repository>
    </repositories>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <java.version>1.8</java.version>
        <maven.compiler.source>${java.version}</maven.compiler.source>
        <maven.compiler.target>${java.version}</maven.compiler.target>
        <flink.version>1.10.0</flink.version>
        <scala.version>2.11.12</scala.version>
        <scala.binary.version>2.11</scala.binary.version>
    </properties>

    <dependencies>
        <!-- 依赖Scala语言 -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-scala_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!-- Apache Flink 的依赖, 这些依赖项，不应该打包到JAR文件中. -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!-- flink操作hdfs，所需要导入该包-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-shaded-hadoop-2-uber</artifactId>
            <version>2.7.5-10.0</version>
        </dependency>

        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.74</version>
        </dependency>

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.12</version>
        </dependency>

        <!-- 添加logging框架, 在IDE中运行时生成控制台输出. -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.7</version>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>log4j</groupId>
            <artifactId>log4j</artifactId>
            <version>1.2.17</version>
            <scope>runtime</scope>
        </dependency>
    </dependencies>

    <build>
        <sourceDirectory>src/main/java</sourceDirectory>
        <testSourceDirectory>src/test/java</testSourceDirectory>
        <plugins>
            <!-- 编译插件 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.5.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <!--<encoding>${project.build.sourceEncoding}</encoding>-->
                </configuration>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>2.18.1</version>
                <configuration>
                    <useFile>false</useFile>
                    <disableXmlReport>true</disableXmlReport>
                    <includes>
                        <include>**/*Test.*</include>
                        <include>**/*Suite.*</include>
                    </includes>
                </configuration>
            </plugin>
            <!-- 打jar包插件(会包含所有依赖) -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <!--
                                        zip -d learn_spark.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF -->
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer
                                        implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <!-- 可以设置jar包的入口类(可选) -->
                                    <!--
                                    <mainClass>com.itcast.flink.batch.FlinkBatchWordCount</mainClass>
                                    -->
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

```

























