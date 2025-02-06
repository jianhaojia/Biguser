---
stypora-copy-images-to: img
typora-root-url: ./
---



# Flink_Day06：Flink Table与SQL及综合案例



![1602831101602](/img/1602831101602.png)



## 01-[复习]-上次课程内容回顾 

> 主要讲解：Flink框架四大基石之==状态State及检查点Checkpoint，端到端一次性语义==和异步IO请求。

![1615338086047](/img/1615338086047.png)

```
1、State 状态
    流式计算引擎独有，比较重要的，比如累加金额
    组织形式：ManagedState、RawState
    类型划分：OperatorState（通常Source）、KeyedState（keyBy分组）
    数据类型：
        ValueState、MapState、ListState、BroadcastState（1.5）、AggregateState等
    状态后端Statebackend：
        MemoryBackend
            State -> TM 内存，Checkpoint -> JM内存
        FsBackend
            State -> TM 内存，Checkpoint -> 可靠文件系统，比如HDFS
        RocksDBBackend
            State -> RocksDB，Checkpoint -> 可靠文件系统，比如HDFS
            
2、Checkpoint 检查点
    将某时刻State进行快照，并且保存到可靠文件系统上
    流程（简易、详细）：
        栅栏barrier
        Checkpoint Coordinator协调器
        分布式快速算法
    手动恢复
    自动容灾恢复，可以设置重启策略
    SavePoint，保存点，人为对Flink Job进行Checkpoint，用于应用升级，有计划操作

3、端到端精确性一次性语义
    流式数据处理中语义
        最多一次、至少一次、精确一次、端到端精确一次
    Flink 如何实现端到端精确一次
        Source 数据源：可恢复读取位置，比如偏移量
        Transformation 转换操作：Checkpoint 检查点
        Sink：幂等性、事务性（2PC、WAL预写日志）
    Flink与Kafka集成
        End-to-End 精确一次性语义实现
    Flink与MySQL集成
        End-to-End 精确一次性语义实现，考虑Sink基于2PC 
        Flink 1.11 提供jdbcSink完成数据写入操作
            At leaest Once + Upsert/幂等性

4、异步IO
    数据流中每条数据，需要请求外部存储系统，为例提高性能，采用异步请求方式
    Flink 1.2提供功能 
    满足条件：
        1. 外部存储支持异步客户端
        2. 如果不支持异步客户端，可以使用线程池进行请求
        MySQL或Redis数据库,可以使用Vert.x框架进行异步请求
```





## 02-[了解]-第6天：课程内容提纲

> 主要讲解Flink计算引擎高级知识点、Flink Table API（DSL编程）和SQL、综合案例（实时性）。

```
1、了解Flink流计算或批处理其他层次API使用
	- 最底层API，流计算
		针对数据进行状态处理，process方法
		
	- Flink Table API & SQL
		相当于SparkSQL模块：SQL编程和DSL编程
		抛砖引玉，1.12版成熟稳定，建议使用
		
2、广播状态：BroadcastState
	事实表（大表）
					实时关联JOIN
	维度表（小表）


3、实时综合案例
	模拟产生交易订单数据，发送到Kafka Topic中，编写Flink 程序，实时进行统计分析
		1. 总销售额
		2. 各省份销售额
		3. 重点城市销售额
	实时指标存储MySQL数据库表
	

4、Flink 运行原理
	Flink Job如何调度执行，很多概念，面试常问。
```





## 03-[理解]-Flink API之ProcessFunction 使用

> [Flink DataStream API中最底层API，提供`process`方法，其中需要实现`ProcessFunction`函数]()

![1615294793065](/img/1615294793065.png)



> 查看抽象类：`ProcessFunction`源码，最主要方法：`processElement`，对流中每条数据进行处理。

![1630510940515](/img/1630510940515.png)



> 案例演示，使用process函数，代替filter函数。

```java
package cn.itcast.flink.process;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 使用Flink 计算引擎实现流式数据处理：从Socket接收数据，实时进行词频统计WordCount
 */
public class _01FlinkStreamProcessDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
		SingleOutputStreamOperator<String> filterStream = inputDataStream.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String line) throws Exception {
				return null != line && line.trim().length() > 0;
			}
		});
		filterStream.printToErr();

		// TODO: 可以使用底层API方法 -> process
		SingleOutputStreamOperator<String> processStream = inputDataStream.process(new ProcessFunction<String, String>() {
			// TODO:  表示处理流中每条数据,通过out进行输出
			@Override
			public void processElement(String line, Context ctx, Collector<String> out) throws Exception {
				if(null != line && line.trim().length() > 0){
					out.collect(line);
				}
			}
		});
		processStream.print();

		// 4. 数据终端-sink

		// 5. 执行应用-execute
		env.execute("FlinkStreamWordCount");
	}

}

```







## 04-[了解]-Table API & SQL之概述及发展史

> ​			在Flink 流式计算引擎中，提供`Flink Table API & SQL`模块，类似SparkSQL模块，提供高层次API，以便用户使用，开发程序更加简单。

![1615294793065](/img/1615294793065.png)

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/

![1615341101430](/img/1615341101430.png)

​		

> ​			Flink Table API& SQL 是一种关系型API，用户可以像操作MySQL数据库表一样的操作数据，而不需要写Java代码完成Flink function，更不需要手工的优化Java代码调优。

```
Flink Table API & SQL 发展史：
1、Flink 1.9版本之前
	Table API & SQL 发展不是很迅速，企业使用也不多
	API使用相对比较复杂
	底层优化性能也不是很好，分别针对批处理和流计算进行设计优化引擎

----------------  2019年，阿里巴巴收购Flink 母公司 ----------------
	Apache Flink  ->  Blink（阿里巴巴内部Flink） ->  Flink Table API & SQL，进行重构和优化

	Apache Flink	
						->    Apache Flink（整合），发布第一个版本Flink 1.9版本
		   Blink
			

2、Flink 1.9版本
	Table API & SQL  底层引擎（查询计划器）：Flink Planner和Blink Planner
	陆续发展Table API & SQL模块，发布
		Flink 1.10版本（相当稳定）
		Flink 1.11版本（过渡版本）
		
3、Flink 1.12版本（里程碑版本）
	Flink Table API & SQL 基本功能完善
	API接口进行重构，使用Blink底层查询处理器
	推荐使用Table API & SQL模块在实际项目中使用
	
	
```



![1615341611151](/img/1615341611151.png)

> 在Flink 1.9版本中，Blink中Table 模块合并到ApacheFlink中，架构进行全新调整。

![1615341649177](/img/1615341649177.png)

> ​		在Flink1.9之后新的架构中，有两个查询处理器：`Flink Query Processor`，也称作Old Planner和`Blink Query Processor`，也称作`Blink Planner`。

![1615341800528](/img/1615341800528.png)



​			当前使用Flink版本（1.10）中提供Table API 和SQL模块，属于测试版本，不建议在生产环境中使用。

![1615341856712](/img/1615341856712.png)







## 05-[掌握]-Table API & SQL之入门案例【环境准备 】

​				以案例形式，讲解Table API和SQL 基本使用，分别针对批处理和流计算使用Table API和SQL分析数据。

> [首先看一下Flink Table API和SQL使用，构建应用步骤。]()

- 第一步、添加依赖

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge_2.11</artifactId>
    <version>1.10.0</version>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner_2.11</artifactId>
    <version>1.10.0</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_2.11</artifactId>
    <version>1.10.0</version>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-common</artifactId>
    <version>1.10.0</version>
    <scope>provided</scope>
</dependency>
```



- 第2步、具体提供API

	 ​		目前新版本（当前使用版本Flink 1.10）Flink的Table和SQL的API还不够稳定，依然在不断完善中，所以课程中的案例还是以`老版本文档的API`来演示。
>
> - ==1）、获取环境==
>   - 批处理Batch：`ExecutionEnvironment`和`BatchTableEnvironment`
>   - 流计算Stream：`StreamExecutionEnvironment`和`StreamTableEnvironment`

![1615342247807](/img/1615342247807.png)



> 官方案例演示代码：

![1615342382812](/img/1615342382812.png)





> - **2）、程序结构**
>   - 1）将DataStream数据集转换为Table或者注册为临时视图或表。
>   - 2）编写Table API或SQL分析数据 - Table
>   - 3）将结果数据Table 转换为 DataStream

![1629430936543](/img/1629430936543.png)

上述流程代码案例如下所示:

![1615342431355](/img/1615342431355.png)





## 06-[理解]-Table API & SQL之入门案例【Batch SQL 】 

> 批处理：==基于Flink Table API和SQL实现词频统计WordCount程序==

![1615342649994](/img/1615342649994.png)

​			为了方便处理，将数据封装在`WordCount`实体类，代码如下所示：

```java
package cn.itcast.flink.start.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
    private String word ;
    private Long counts;
}
```

> 编程实现批处理词频统计WordCount，采用SQL方式实现，具体代码如下：

```scala
package cn.itcast.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Flink SQL API 针对批处理实现词频统计WordCount
 */
public class BatchWordCountSQLDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO: 获取Table执行环境
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

		// 2. 数据源-source
		// 模拟测试数据集
		DataSource<WordCount> inputDataSet = env.fromElements(
			new WordCount("flink", 1L), new WordCount("flink", 1L),
			new WordCount("spark", 1L), new WordCount("spark", 1L),
			new WordCount("flink", 1L), new WordCount("hive", 1L),
			new WordCount("flink", 1L), new WordCount("spark", 1L)
		);
		// TODO: 将DataStream转换为Table
		tableEnv.createTemporaryView("word_count", inputDataSet, "word, counts");

		// 3. 数据转换-transformation
		// TODO: 编写SQL分析数据
		Table wcTable = tableEnv.sqlQuery("SELECT word, SUM(counts) AS counts FROM word_count GROUP BY word ORDER BY counts DESC");

		// TODO: 转换Table为DataSet
		DataSet<WordCount> resultDataSet = tableEnv.toDataSet(wcTable, WordCount.class);

		// 4. 数据终端-sink
		resultDataSet.printToErr();

		// 5. 触发执行-execute
		//env.execute(BatchWordCountSQLDemo.class.getSimpleName()) ;
	}

}
```





## 07-[理解]-Table API & SQL之入门案例【Batch Table API 】

> Flink Table API 针对批处理实现词频统计WordCount，具体代码如下：

```java
package cn.itcast.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Flink SQL API 针对批处理实现词频统计WordCount
 */
public class BatchWordCountTableDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO: 获取Table执行环境
		BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

		// 2. 数据源-source
		// 模拟测试数据集
		DataSource<WordCount> inputDataSet = env.fromElements(
			new WordCount("flink", 1L), new WordCount("flink", 1L),
			new WordCount("spark", 1L), new WordCount("spark", 1L),
			new WordCount("flink", 1L), new WordCount("hive", 1L),
			new WordCount("flink", 1L), new WordCount("spark", 1L)
		);
		// TODO: 将DataStream转换为Table
		Table table = tableEnv.fromDataSet(inputDataSet);

		// 3. 数据转换-transformation
		// TODO: 编写DSL分析数据
		/*
			Flink Table API使用，就是将SQL语句，拆解以后，放到对应函数中
				SELECT word, SUM(counts) AS counts FROM word_count GROUP BY word ORDER BY counts DESC
		 */
		Table resultTable = table
			.groupBy("word") // 按照什么分组
			.select("word, SUM(counts) AS counts") // 选择字段
			.orderBy("counts.desc");

		// TODO: 转换Table为DataSet
		DataSet<WordCount> resultDataSet = tableEnv.toDataSet(resultTable, WordCount.class);

		// 4. 数据终端-sink
		resultDataSet.printToErr();

		// 5. 触发执行-execute
		//env.execute(BatchWordCountSQLDemo.class.getSimpleName()) ;
	}

}

```







## 08-[理解]-Table API & SQL之入门案例【Stream SQL】

> 使用Flink Table API和SQL分别对流计算编写入门案例。

![1615344828807](/img/1615344828807.png)

> ​		在流计算中，依然需要将DataStream数据流转换为Table，进行DSL和SQL编程，最后将Table结果转换DataStream进行最后输出操作。
>

```java
package cn.itcast.flink.stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * Flink SQL流式数据处理案例演示，官方Example案例。
 */
public class StreamSQLDemo {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Order {
		public Long user;
		public String product;
		public Integer amount;
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO: 构建Stream Table 执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. 数据源-source
		// 模拟数据集
		DataStream<Order> orderA = env.fromCollection(Arrays.asList(
			new Order(1001L, "beer", 3),
			new Order(1001L, "diaper", 4),
			new Order(1003L, "rubber", 2)
		));
		DataStream<Order> orderB = env.fromCollection(Arrays.asList(
			new Order(1002L, "pen", 3),
			new Order(1002L, "rubber", 3),
			new Order(1004L, "beer", 1)
		));

		// TODO: 将DataStream转换Table
		Table tableA = tableEnv.fromDataStream(orderA, "user, product, amount");
		tableEnv.createTemporaryView("orderB", orderB, "user, product, amount");

		// 3. 数据转换-transformation
		//使用SQL查询数据，分别对2个表进行数据查询，将结果合并
		Table resultTable = tableEnv.sqlQuery(
			"SELECT * FROM " + tableA + " WHERE amount > 2 union all SELECT * FROM orderB WHERE amount > 2 "
		);
		// 将Table转换为DataStream
		DataStream<Order> resultDataStream = tableEnv.toAppendStream(resultTable, Order.class);

		// 4. 数据终端-sink
		resultDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamSQLDemo.class.getSimpleName()) ;
	}

}
```



## 09-[理解]-Table API & SQL之入门案例【Stream Table API】

> Flink Table API使用，基于==事件时间窗口==统计分析，数据集如下：

```
1,beer,3,2020-12-12 00:00:01
1,diaper,4,2020-12-12 00:00:02
2,pen,3,2020-12-12 00:00:04
2,rubber,3,2020-12-12 00:00:06
3,rubber,2,2020-12-12 00:00:05
4,beer,1,2020-12-12 00:00:08

2,rubber,3,2020-12-12 00:00:10
3,rubber,2,2020-12-12 00:00:10
```

> 此程序涉及到基于事件时间窗口统计，所以需要主要使用方式：`rowtime`表示就是事件时间字段。

```java
package cn.itcast.flink.stream;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.ParseException;

/**
 * Flink Table API使用，基于事件时间窗口统计分析：
 *
 */
public class StreamWindowTableDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// step1. 设置时间语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// TODO: 创建 Stream Table执行环境
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// 2. 数据源-source
		DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
1,beer,3,2020-12-12 00:00:01
1,diaper,4,2020-12-12 00:00:02
2,pen,3,2020-12-12 00:00:04
2,rubber,3,2020-12-12 00:00:06
3,rubber,2,2020-12-12 00:00:05
4,beer,1,2020-12-12 00:00:08

2,rubber,3,2020-12-12 00:00:10
3,rubber,2,2020-12-12 00:00:10
 */
		// step2. 指定事件时间字段，必须是Long类型时间戳，考虑乱序数据处理（水位线Watermark）
		FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
		SingleOutputStreamOperator<String> timeStream = inputStream
			.filter(line -> null != line && line.trim().split(",").length == 4)
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
				@Override
				public long extractTimestamp(String line) {
					String orderTime = line.trim().split(",")[3];
					try {
						return format.parse(orderTime).getTime();
					} catch (ParseException e) {
						e.printStackTrace();
						return System.currentTimeMillis();
					}
				}
			});
		// 提取字段数据
		SingleOutputStreamOperator<Row> orderStream = timeStream.map(new MapFunction<String, Row>() {
			@Override
			public Row map(String line) throws Exception {
				// 数据格式：2,rubber,3,2020-12-12 00:00:10
				String[] split = line.trim().split(",");
				String userId = split[0];
				String productName = split[1];
				Integer amount = Integer.parseInt(split[2]);
				Long orderTime = format.parse(split[3]).getTime();
				// 返回对象，Row类型
				return Row.of(orderTime, userId, productName, amount);
			}
		}).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.INT)) ;
		// TODO：将DataStream转换Table
		tableEnv.createTemporaryView(
			"t_orders",//
			orderStream, //
			"order_time, user_id, product_name, amount, event_time.rowtime"
		);

		Table resultTable = tableEnv
			.from("t_orders")
			// 设置基于事件时间滚动窗口
			.window(Tumble.over("5.seconds").on("event_time").as("win"))
			// 先对窗口分组，再对窗口中数据按照用户
			.groupBy("win, user_id")
			// 聚合计算
			.select("win.start, win.end, user_id, amount.sum as total");

		// TODO: 将DataStream转换为Table
		DataStream<Row> resultStream = tableEnv.toAppendStream(resultTable, Row.class);

		// 4. 数据终端-sink
		resultStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamWindowTableDemo.class.getSimpleName()) ;
	}

}

```





## 10-[理解]-Broadcast State之功能概述

```
BroadcastState：
	Broadcast，表示广播意思
	在SparkCore或Flink DataSet批处理中，广播变量，将小数据集广播出去，被Task共享使用
	
	
在批处理中，小表数据：DataSet
	广播变量
在流计算中，小表数据：DataStream
	广播流   ->  通过BroadcastState广播
	
	
场景：
	有2个DataStream流
        第一流、大数据流，事实表的数据流
        	比如行为日志数据clickLogStream
        第二流、小数据流，维度表数据流
        	比如MySQL数据库中用户信息表数据userInfoStream
        	
	由于小数据流数据会出现变化，比如添加新的数据、更新老的数据
		大数据流中每条数据需要关联到小数据流中数据信息，比如实时业务数据拉宽操作
		订单表与商品表实时拉宽操作
		
	此时可以将小数据流采用BroadcastState将其广播到大数据流中进行使用
		即使小数据流中数据变化了，大数据流数据在进行处理时，也可以即使感应到
```



> ​			**Broadcast State** 是 Flink 1.5 引入的新特性。在开发过程中，如果遇到**需要==下发/广播==配置、规则等==低吞吐事件流==到下游所有 task 时，就可以使用 Broadcast State 特性**。下游的 task 接收这些配置、规则并保存为 BroadcastState, 将这些配置应用到另一个数据流的计算中 。

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/broadcast_state.html

![1626994736306](/img/1626994736306.png)

> 使用Flink中广播状态`BroadcastState`，可以实时更新数据如下示意图：
>
> - 1）、大表数据，实时产生：用户访问网站日志数据log表
> - 2）、小表数据，维度表，需要与大表数据实时关联，进行拉宽操作。
>

[			小表数据存储在MySQL表中，大表数据来源于Kafka消息队列（代码自定义数据源），小表数据可能会被更新，比如新用户注册，访问网站，产生日志数据]()

![1615294767753](/img/1615294767753.png)



> 使用Flink提供State：==BroadcastState状态，将小表数据（维度表）进行广播操作，可以自动更新表中的数据==
>

![1615365504686](/img/1615365504686.png)

> ​			将大表数据流与小表数据流，采用广播状态BroadcastState方式广播小表数据流以后，调用`connect`方法，将两个流数据进行关联。
>
> [connect方法，将两个流（数据类型可以不一样）进行关联，分别对流中数据处理。]()

![1630551157459](/img/1630551157459.png)





## 11-[理解]-Broadcast State之实战案例

​			自定义数据源，实时产生用户行为日志数据和加载MySQL表数据。

> - 1）、实时产生用户行为日志数据：`TrackLogSource`和`TrackLog`

```java
// ============================= 用户行为日志数据 =====================================
package cn.itcast.flink.dynamic;

import lombok.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TrackLog {

	private String userId ;
	private Integer productId ;
	private String trackTime ;
	private String eventType ;

	@Override
	public String toString() {
		return userId + ", " + productId + ", " + trackTime + ", " + eventType;
	}
}


// ============================= 自定义数据源=====================================
package cn.itcast.flink.dynamic;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时产生用户访问网站点击流数据
 */
public class TrackLogSource extends RichParallelSourceFunction<TrackLog> {
	private boolean isRunning = true ;

	@Override
	public void run(SourceContext<TrackLog> ctx) throws Exception {
		String[] types = new String[]{
			"click", "browser", "search", "click", "browser", "browser", "browser",
			"click", "search", "click", "browser", "click", "browser", "browser", "browser"
		} ;
		Random random = new Random() ;
		FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss:SSS") ;

		while (isRunning){
			TrackLog clickLog = new TrackLog(
				"user_" + (random.nextInt(4) + 1), //
				10000 + random.nextInt(10000), //
				format.format(System.currentTimeMillis()), //
				types[random.nextInt(types.length)]
			);
			ctx.collect(clickLog);

			// 每个1秒生成一条数据
			TimeUnit.MILLISECONDS.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning = false ;
	}

}
```



> - 2）、实时加载MySQL数据库表的数据：`UserInfoSource`和`UserInfo`

```java
// =============================  用户基本信息数据 =====================================
package cn.itcast.flink.dynamic;

import lombok.*;

@Setter
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class UserInfo {

	private String userId ;
	private String userName ;
	private Integer userAge ;

	@Override
	public String toString() {
		return userId + ", " + userName + ", " + userAge ;
	}
}

// ================  自定义数据源，从MySQL表加载数据 ================
package cn.itcast.flink.dynamic;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源，实时从MySQL表获取数据，实现接口RichSourceFunction
 */
public class UserInfoSource extends RichSourceFunction<UserInfo> {

	// 标识符，是否实时接收数据
	private boolean isRunning = true;

	private Connection conn = null;
	private PreparedStatement pstmt = null;
	private ResultSet rs = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 1. 加载驱动
		Class.forName("com.mysql.jdbc.Driver");
		// 2. 创建连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8",
			"root",
			"123456"
		);
		// 3. 创建PreparedStatement
		pstmt = conn.prepareStatement("select userId, userName, userAge from db_flink.user_info");
	}

	@Override
	public void run(SourceContext<UserInfo> ctx) throws Exception {
		while (isRunning) {
			// 1. 执行查询
			rs = pstmt.executeQuery();
			// 2. 遍历查询结果,收集数据
			while (rs.next()) {
				String id = rs.getString("userId");
				String name = rs.getString("userName");
				Integer age = rs.getInt("userAge");
				UserInfo userInfo = new UserInfo(id, name, age);
				// 输出
				ctx.collect(userInfo);
			}
			// 每隔3秒查询一次
			TimeUnit.SECONDS.sleep(3);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		if (null != rs) r.close();
		if (null != pstmt) pstmt.close();
		if (null != conn) conn.close();
	}

}
```

> 实现代码如下：

```ini
-- 在MySQL数据库中创建Database和Table

CREATE DATABASE IF NOT EXISTS db_fink ;

USE db_fink ;

DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info` (
 `userID` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
 `userName` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
 `userAge` int(11) NULL DEFAULT NULL,
 PRIMARY KEY (`userID`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;


INSERT INTO `user_info` VALUES ('user_1', '张三', 10);
INSERT INTO `user_info` VALUES ('user_2', '李四', 20);
INSERT INTO `user_info` VALUES ('user_3', '王五', 30);
INSERT INTO `user_info` VALUES ('user_4', '赵六', 40);
```



编写Flink Stream流式应用程序，将小表数据广播，每隔1秒更新一下，此时大表数据可以与小表关联。

```scala
package cn.itcast.flink.broadcast;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息
 *      TODO: 用户信息存储在MySQL数据库表
 *  实时将大表与小表数据进行关联，其中小表数据动态变化
 *      大表数据：流式数据，存储Kafka消息队列
 *      小表数据：动态数据，存储MySQL数据库
 *   TODO： BroadcastState 将小表数据进行广播，封装到Map集合集合中，使用connect函数与大表数据流进行连接
 */
public class _06FlinkBroadcastStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		// 2-1. 构建实时数据事件流: 用户行为日志，<userId, productId, trackTime, eventType>
		DataStreamSource<TrackLog> logStream = env.addSource(new TrackLogSource());
		//logStream.print();

		// 2-2. 构建配置流: 用户信息，<userId, name, age>
		DataStreamSource<UserInfo> userStream = env.addSource(new UserInfoSource());
		//userStream.printToErr();

		// 3. 数据转换-transformation
		// 3-1. 定义状态State描述符
		MapStateDescriptor<String, UserInfo> descriptor = new MapStateDescriptor<>(
			"userInfoState", Types.STRING, TypeInformation.of(new TypeHint<UserInfo>() {}) //
		);
		// 3-2. 将小表数据（用户信息数据流）进行广播
		BroadcastStream<UserInfo> broadcastStream = userStream.broadcast(descriptor);

		// 3-3. 大表数据调用connect连接方法，关联广播小表数据
		SingleOutputStreamOperator<String> connectStream = logStream
			.connect(broadcastStream)
			.process(new BroadcastProcessFunction<TrackLog, UserInfo, String>() {
				// 对大表数据进行操作
				@Override
				public void processElement(TrackLog trackLog,
				                           ReadOnlyContext ctx,
				                           Collector<String> out) throws Exception {
					// 获取userId
					String userId = trackLog.getUserId();
					// 获取广播状态，就是小表数据
					ReadOnlyBroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
					// 依据用户Id获取用户信息
					UserInfo userInfo = broadcastState.get(userId);
					// 组合日志数据和用户数据，进行输出
					if(null != userInfo){
						// 拼接字符串
						String output = trackLog.toString() + " <-> " + userInfo.toString() ;
						out.collect(output);
					}
				}

				// 对小表数据进行操作，将获取MySQL数据库每个用户信息，放到广播状态中
				@Override
				public void processBroadcastElement(UserInfo userInfo,
				                                    Context ctx,
				                                    Collector<String> out) throws Exception {
					// 获取用户ID
					String userId = userInfo.getUserId();
					// 获取广播状态对象，将用户信息进行设置
					BroadcastState<String, UserInfo> broadcastState = ctx.getBroadcastState(descriptor);
					// 设置用户信息
					broadcastState.put(userId, userInfo);
				}
			});

		// 4. 数据终端-sink
		connectStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_06FlinkBroadcastStateDemo.class.getSimpleName());
	}

}
```





## 12-[掌握]-实时综合实战之业务需求

> ​		每年天猫双十一购物节，都会有一块巨大的实时作战大屏，展现当前的销售情况。这种炫酷的页面背后，其实有着非常强大的技术支撑，而这种场景其实就是实时报表分析。

![1626995351992](/img/1626995351992.png)

​		在大数据实时计算方向，天猫双十一的实时交易额是最具权威性的，当然技术架构也是相当复杂的，因为天猫双十一的数据是多维度多系统，实时粒度更微小的，主要组件是大数据实时计算组件Flink（从2017年开始），实现实时报表分析及实时数据存储。



> ​		实时从Kafka消费交易订单数据，按照不同维度实时统计【销售订单额】，最终报表Report结果存储内存数据库Redis或MySQL数据库。具体报表需求，包含三个维度，如下所示：

![1626995473425](/img/1626995473425.png)

```
报表存储MySQL据库，每类指标存储1张表中：
	1、总销售额：tbl_order_report_all
	2、各省份销售额：tbl_order_report_province
	3、重点城市消费额：tbl_order_report_city
	
运行模拟产生交易订单数据程序；
{
  "orderId": "20210902111904950000001",
  "userId": "300000609",
  "orderTime": "2021-09-02 11:19:04.950",
  "ip": "139.215.242.192",
  "orderMoney": 118.03,
  "orderStatus": 0
}


1、获取订单状态为0数据
	表示订单刚刚生成
2、省份和城市
	解析IP地址获取省份和城市
3、Double类型数据，不允许直接相加或相减，丢失精度
	Double转换BigDecimal类型
```

> 编写Flink Stream流式应用程序，代码结构如下：

![1630538288484](/img/1630538288484.png)

```java
/**
 * 实时订单报表：从Kafka Topic实时消费订单数据，进行销售订单额统计，结果实时存储MySQL数据库，维度如下：
	 * - 第一、总销售额：sum
	 * - 第二、各省份销售额：province
	 * - 第三、重点城市销售额：city
	 *      "北京市", "上海市", "深圳市", "广州市", "杭州市", "成都市", "南京市", "武汉市", "西安市"
 */
public class _07RealTimeOrderReport {
	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		// 2. 数据源-source：从Kafka加载数据
		DataStream<String> kafkaStream = kafkaSource(env, "orderTopic");
		//kafkaStream.printToErr();

		// 3. 数据转换-transformation
		// 3-1. TODO: 提取相关数据字段，依据订单状态过滤数据，并解析IP地址为省份和城市
		DataStream<Tuple3<String, String, BigDecimal>> orderStream = streamEtl(kafkaStream) ;
		//orderStream.print();

		// 3-2. TODO：实时报表统计：总销售额、各省份销售额及重点城市销售额
		// 第一、总销售额
		DataStream<Tuple2<String, BigDecimal>> reportAllStream = reportAll(orderStream) ;
		reportAllStream.printToErr("all>>>>>>>>>>>").setParallelism(1);

		// 第二、各省份销售额
		DataStream<Tuple2<String, BigDecimal>> reportProvinceStream = reportProvince(orderStream) ;
		//reportProvinceStream.print("province>>>>>>>>>>>").setParallelism(1);

		// 第三、重点城市销售额
		DataStream<Tuple2<String, BigDecimal>> reportCityStream = reportCity(orderStream) ;
		//reportCityStream.printToErr("city>>>>>>>>>>>").setParallelism(1);

		// 4. 数据终端-sink，TODO: 将数据保存到MySQL数据库: JdbcSink
		jdbcSink(reportAllStream, "tbl_order_report_all");
		//jdbcSink(reportProvinceStream, "tbl_order_report_province");
		//jdbcSink(reportCityStream, "tbl_order_report_city");

		// 5. 触发执行
		env.execute("RealTimeOrderReport");
	}

}	
```



## 13-[掌握]-实时综合实战之实时加载数据

> 编写方法：`kafkaSource`，从Kafka实时消费交易订单数据，代码如下。

```java
	/**
	 * 从Kafka实时消费数据，返回DataStream，数据类型为String
	 */
	private static DataStream<String> kafkaSource(StreamExecutionEnvironment env, String topic){
		// a. 从Kafka消费数据，指定属性参数
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "node1.itcast.cn:9092,node2.itcast.cn:9092,node3.itcast.cn:9092");
		props.setProperty("group.id", "test-1002");
		// b. 构建消费者对象
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
			topic, new SimpleStringSchema(), props
		);
		// c.添加数据源
		return env.addSource(kafkaConsumer);
	}
```





## 14-[掌握]-实时综合实战之数据ETL转换

> 编写方法：`streamEtl`，对数据进行转换解析处理，代码如下：
>
> - a. 解析JSON数据，封装实体类对象
> - b. 过滤订单状态为0的数据
> - c. 解析IP地址为省份和城市

```java
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class OrderData{
		String orderId ;
		String userId ;
		String orderTime ;
		String ip ;
		Double orderMoney ;
		Integer orderStatus ;
	}

	/**
	 * 解析从Kafka消费获取的交易订单数据，过滤订单状态为0（打开）数据，并解析IP地址为省份和城市
	 *      a. 解析JSON数据，封装实体类对象
	 * 		b. 过滤订单状态为0的数据
	 * 	    c. 解析IP地址为省份和城市
	 */
	private static DataStream<Tuple3<String, String, BigDecimal>> streamEtl(DataStream<String> stream){
		// 数据处理转换
		SingleOutputStreamOperator<Tuple3<String, String, BigDecimal>> orderStream = stream
			// a. 解析JSON数据，封装实体类对象
			.map(new MapFunction<String, OrderData>() {
				@Override
				public OrderData map(String msg) throws Exception {
					return JSON.parseObject(msg, OrderData.class);
				}
			})

			// b. 过滤订单状态为0的数据
			.filter(new FilterFunction<OrderData>() {
				@Override
				public boolean filter(OrderData orderData) throws Exception {
					return orderData.orderStatus == 0;
				}
			})
			// c. 解析IP地址为省份和城市，并提取订单金额，返回三元组
			.map(new MapFunction<OrderData, Tuple3<String, String, BigDecimal>>() {
				@Override
				public Tuple3<String, String, BigDecimal> map(OrderData orderData) throws Exception {
					// 解析IP地址为省份和城市
					DbSearcher dbSearcher = new DbSearcher(new DbConfig(), "dataset/ip2region.db");
					DataBlock dataBlock = dbSearcher.btreeSearch(orderData.ip);
					String[] split = dataBlock.getRegion().split("\\|");
					// 省份和城市
					String province = split[2];
					String city = split[3];
					// 订单金额
					BigDecimal decimal = new BigDecimal(orderData.orderMoney).setScale(2, RoundingMode.HALF_UP);
					// 返回数据
					return Tuple3.of(province, city, decimal);
				}
			});
		// 返回数据流
		return orderStream ;
	}
```

> 流式数据处理时2个重点：
>
> - JSON数据转换，如果使用Java语言开发，推荐使用`FastJson`库或者`Gson`库。
> - Double数值类型sum求和操作，必须要转换数据类型为`BigDecimal`。

此外，离线解析IP地址库：Ip2Region，非常好用。

```
dependency>
    <groupId>org.lionsoul</groupId>
    <artifactId>ip2region</artifactId>
    <version>1.4</version>
</dependency>
```

https://gitee.com/lionsoul/ip2region

![1630555464903](/img/1630555464903.png)





## 15-[掌握]-实时综合实战之总销售额

> 编写方法：`reportAll`，实时统计全国总销售额，代码如下：

```java
	/**
	 * 实时报表统计：总销售额
	 */
	private static DataStream<Tuple2<String, BigDecimal>> reportAll(DataStream<Tuple3<String, String, BigDecimal>> stream){
		// 分组、聚合统计
		SingleOutputStreamOperator<Tuple2<String, BigDecimal>> reportStream = stream
			// 提取金额字段，返回二元组
			.map(new MapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> tuple) throws Exception {
					return Tuple2.of("全国", tuple.f2);
				}
			})
			// 按照全国分组
			.keyBy(0)
			// 组内求和
			.reduce(new ReduceFunction<Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp,
				                                         Tuple2<String, BigDecimal> item) throws Exception {
					return Tuple2.of(temp.f0, temp.f1.add(item.f1));
				}
			});
		// 返回销售额
		return reportStream;
```





## 16-[掌握]-实时综合实战之各省份销售额

> 编写方法：`reportProvince`，实时统计各省份销售额，代码如下：
>

```java
/**
	 * 实时报表统计：各省份销售额
	 */
	private static DataStream<Tuple2<String, BigDecimal>> reportProvince(DataStream<Tuple3<String, String, BigDecimal>> stream){
		// 分组、聚合统计
		SingleOutputStreamOperator<Tuple2<String, BigDecimal>> reportStream = stream
			// 提取省份和金额
			.map(new MapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> tuple) throws Exception {
					return Tuple2.of(tuple.f0, tuple.f2);
				}
			})
			// 按照省份分组
			// 组内求和
			.keyBy(0)
			.reduce(new ReduceFunction<Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp,
				                                         Tuple2<String, BigDecimal> item) throws Exception {
					return Tuple2.of(temp.f0, temp.f1.add(item.f1));
				}
			});
		// 返回销售额
		return reportStream;
	}

```





## 17-[掌握]-实时综合实战之重点城市销售额

> 如下重点城市，进行实时消费额统计：

```java
		// 重点城市
		List<String> cities = new ArrayList<String>() { //
			{ //
				add("北京市") ;
				add("上海市") ;
				add("深圳市") ;
				add("广州市") ;
				add("杭州市") ;
				add("成都市") ;
				add("南京市") ;
				add("武汉市") ;
				add("西安市") ;
			}//
		} ;
```



> 编写方法：`reportCity`，实时统计各重点城市销售额，代码如下：
>

```java
	/**
	 * 实时报表统计：重点城市销售额
	 */
	private static DataStream<Tuple2<String, BigDecimal>> reportCity(DataStream<Tuple3<String, String, BigDecimal>> stream){
		// 重点城市
		List<String> cities = new ArrayList<String>() { //
			{ //
				add("北京市") ;
				add("上海市") ;
				add("深圳市") ;
				add("广州市") ;
				add("杭州市") ;
				add("成都市") ;
				add("南京市") ;
				add("武汉市") ;
				add("西安市") ;
			}//
		} ;
		// 分组、聚合统计
		SingleOutputStreamOperator<Tuple2<String, BigDecimal>> reportStream = stream
			// 提取城市和金额
			.map(new MapFunction<Tuple3<String, String, BigDecimal>, Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> map(Tuple3<String, String, BigDecimal> tuple) throws Exception {
					return Tuple2.of(tuple.f1, tuple.f2);
				}
			})
			// 过滤重点城市
			.filter(new FilterFunction<Tuple2<String, BigDecimal>>() {
				@Override
				public boolean filter(Tuple2<String, BigDecimal> tuple) throws Exception {
					return cities.contains(tuple.f0);
				}
			})
			// 按照省份分组
			.keyBy(0)
			// 组内求和
			.reduce(new ReduceFunction<Tuple2<String, BigDecimal>>() {
				@Override
				public Tuple2<String, BigDecimal> reduce(Tuple2<String, BigDecimal> temp,
				                                         Tuple2<String, BigDecimal> item) throws Exception {
					return Tuple2.of(temp.f0, temp.f1.add(item.f1));
				}
			});
		// 返回销售额
		return reportStream;
	}
```





## 18-[掌握]-实时综合实战之保存MySQL数据库

> 采用JdbcSink方式，将各个统计销售额存储到MySQL数据库表中，此处针对每个报表销售额创建1个表。

```SQL

-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_flink ;

-- 使用数据库
USE db_flink ;

-- 创建表
CREATE TABLE db_flink.tbl_order_report_all (
    `type` varchar(255) NOT NULL,
    `amount` decimal(10,0) NOT NULL,
    PRIMARY KEY (`type`),
    UNIQUE KEY `tbl_report_type_name_uindex` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;

CREATE TABLE db_flink.tbl_order_report_province (
    `type` varchar(255) NOT NULL,
    `amount` decimal(10,0) NOT NULL,
    PRIMARY KEY (`type`),
    UNIQUE KEY `tbl_report_type_name_uindex` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;

CREATE TABLE db_flink.tbl_order_report_city (
    `type` varchar(255) NOT NULL,
    `amount` decimal(10,0) NOT NULL,
    PRIMARY KEY (`type`),
    UNIQUE KEY `tbl_report_type_name_uindex` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;
```

> 编写方法：jdbcSink，将分析结果数据流实时存储到MySQL表中，代码如下；

```java
	/**
	 * 将统计结果，保存到MySQL数据库表中
	 */
	private static void jdbcSink(DataStream<Tuple2<String, BigDecimal>> stream, String table){
		// 构建Sink对象，设置插入语句和数据库连接参数
		SinkFunction<Tuple2<String, BigDecimal>> sink = JdbcSink.sink(
			"REPLACE INTO db_flink."+ table +" (type, amount) VALUES (?, ?)",
			new JdbcStatementBuilder<Tuple2<String, BigDecimal>>() {
				@Override
				public void accept(PreparedStatement pstmt,
				                   Tuple2<String, BigDecimal> tuple) throws SQLException {
					pstmt.setString(1, tuple.f0);
					pstmt.setBigDecimal(2, tuple.f1);
				}
			}, //
			new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withDriverName("com.mysql.jdbc.Driver")
				.withUrl("jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false")
				.withUsername("root")
				.withPassword("123456")
				.build()
		);
		// 添加sink
		stream.addSink(sink) ;
	}
```



## 19-[掌握]-Flink 流式计算调度原理

> 入门案例：词频统计WordCount，依据程序代码，构建Streaming DataFlow（流式数据流图）

![1630566270472](/img/1630566270472.png)

> 其中无论是Source、Transformation和Sink都称为Operator算子，每个Operator算子可以设置不同并行度。

![1630566447080](/img/1630566447080.png)

> 每个Operator算子在执行时，就是`Task任务`，对应到每个并行度就称为`SubTask子任务`。



Stream（流）可以在2个Operator之间传输数据，有两种不同Parttern（模式）传输：

> - One-to-One streams：一对一，[类似spark中窄依赖]()

![1630566706682](/img/1630566706682.png)

> - `Redistributing` streams：一对多，[类似spark中宽依赖]()

![1630566756085](/img/1630566756085.png)



> ​		在Flink Job作业执行时，每个Operator就是一个Task任务，每个并行度对应Operator就是一个SubTask子任务，运行在一个线程Thread中。
>
> - 第一个条件：相邻2个Operator并行度（parallelism）相同；
> - 第二个条件：相邻2个Operator之间数据传递方式为One-to-One 模式；

![1630567239799](/img/1630567239799.png)

> 当相邻Operator合并Chain以后，加上并行度，示意图如下：[展示出实际执行Flink Job执行所运行subTask。]()

![1630567347914](/img/1630567347914.png)



> 每个subTask以线程方式运行在TaskManager中Slot资源槽中。

![1630567804688](/img/1630567804688.png)







## [附录]-创建Maven模块

> Maven Module模块工程结构

![1615294217921](/img/1615294217921.png)



> Maven 工程POM文件中内容（依赖包）：

```xml
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
        <mysql.version>5.1.48</mysql.version>
    </properties>

    <dependencies>
        <!-- 依赖Scala语言 -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <dependency>
            <groupId>org.json4s</groupId>
            <artifactId>json4s-jackson_2.11</artifactId>
            <version>3.5.3</version>
        </dependency>

        <!-- Flink Dependencies -->
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
        <!-- Flink Table API and SQL Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-api-java-bridge_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-planner-blink_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- Flink CEP Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-cep_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- Flink Hadoop Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-shaded-hadoop-2-uber</artifactId>
            <version>2.7.5-10.0</version>
        </dependency>
        <!-- Flink connector Kafka Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- Flink connector Redis Dependencies -->
        <dependency>
            <groupId>org.apache.bahir</groupId>
            <artifactId>flink-connector-redis_${scala.binary.version}</artifactId>
            <version>1.0</version>
        </dependency>
        <!-- Flink connector Jdbc Dependencies -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-jdbc_2.11</artifactId>
            <version>1.11.0</version>
        </dependency>
        <!-- MySQL connector Dependencies -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>${mysql.version}</version>
        </dependency>
        <!-- lombok Dependencies -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.12</version>
        </dependency>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.74</version>
        </dependency>
        <!-- 根据ip转换为省市区 -->
        <dependency>
            <groupId>org.lionsoul</groupId>
            <artifactId>ip2region</artifactId>
            <version>1.7.2</version>
        </dependency>
        <!-- slf4j and slf4j Dependencies -->
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

