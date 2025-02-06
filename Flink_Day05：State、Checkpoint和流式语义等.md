---
stypora-copy-images-to: img
typora-root-url: ./
---



# Flink_Day05：State、Checkpoint和流式语义



![1602831101602](/img/1602831101602.png)





## 01-[复习]-上次课程内容回顾 

> 上次主要讲解：Flink ==Window窗口==计算和==Time 时间==语义，重点为**基于事件时间窗口分析**。

![1630372468856](/img/1630372468856.png)





## 02-[了解]-第5天：课程内容提纲

> ​	讲解Flink 四大基石：`状态State`和检查点`Checkpoint`、端到端精确性一次语义和异步IO。

![1615166244049](/img/1615166244049.png)

```
1）、状态State
	状态计算（有状态和无状态）
	State划分：组织形式分类、基本类型分类
	State数据结构
	用户管理状态，案例演示
	
2）、检查点Checkpoint
	State与Checkpoint关系
	Checkpoint执行流程
	状态存储后端StateBackend
	Checkpoint配置及案例演示
	手动重启应用
	自动重启和设置
	保存点Savepoint，认为对State进行Checkpoint
	
3）、端到端一次性语义
	流式数据处理语义
	Flink中如何实现End-To-End Exactly Once
		Kafka->Flink->Kafka（自带实现）
		Kafka->Flink->MySQL（自己实现）2PC 两阶段提交
				
4）、异步IO
	流式数据，请求第三方存储系统，异步请求，处理结果
	阿里巴巴提供功能
```

![1630458284520](/img/1630458284520.png)





## 03-[理解]-Flink State之Flink 状态计算 

![1615171006774](/img/1615171006774.png)

> ​		`Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams.` 

![1615166307238](/img/1615166307238.png)

- `SparkStreaming`流式计算框架，属于无状态计算，需要用户管理状态，比如要实现累加统计，调用==updateStateByKey==或==mapWithState==函数。
- `StructuredStreaming`流式计算框架，属于有状态计算，程序自动管理状态。



> 词频统计WordCount程序，其中词频就是使用State状态进行存储，==每个Key对应一个词频（状态）==。

![1630373726836](/img/1630373726836.png)

![1615166638045](/img/1615166638045.png)



> 在流计算场景中，数据会源源不断的流入Apache Flink系统，每条数据进入Apache Flink系统都会触发计算。

- 如果想进行一个Count聚合计算，那么每次触发计算是将历史上所有流入的数据重新新计算一次，还是每次计算都是在上一次计算结果之上进行增量计算呢？

  [Apache Flink是基于上一次的计算结果进行增量计算的。]()

- 那么问题来了: "上一次的计算结果保存在哪里，保存在内存可以吗？"，答案是[否定的]()，如果保存在内存，在由于网络，硬件等原因造成某个计算节点失败的情况下，上一次计算结果会丢失，在节点恢复的时候，就需要将历史上所有数据（可能十几天，上百天的数据）重新计算一次，

  所以为了避免这种灾难性的问题发生，[Apache Flink 会利用State存储计算结果]()。

```scala
package cn.itcast.flink.start;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink DataStream实现词频统计WordCount，从Socket Source读取数据。
 */
public class StreamWordCount {
	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 2. 数据源-source
		DataStream<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
		// 3-1. 过滤数据，使用filter函数
		DataStream<String> lineStream = inputStream.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String line) throws Exception {
				return null != line && line.trim().length() > 0;
			}
		});
		// 3-2. 转换数据，分割每行为单词
		DataStream<String> wordStream = lineStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String line, Collector<String> out) throws Exception {
				String[] words = line.trim().toLowerCase().split("\\s+");
				for (String word : words) {
					out.collect(word);
				}
			}
		});
		// 3-3. 转换数据，每个单词变为二元组
		DataStream<Tuple2<String, Integer>> tupleStream = wordStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(String word) throws Exception {
				return Tuple2.of(word, 1);
			}
		});
		// 3-4. 分组
		DataStream<Tuple2<String, Integer>> resultStream = tupleStream
			.keyBy(0)
			.sum(1);

		// 4. 数据终端-sink
		resultStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamWordCount");
	}
}
```



## 04-[理解]-Flink State之什么是状态 

> ​	==什么是状态==：流式计算的数据往往是转瞬即逝， 真实业务场景不可能说所有的数据都是进来之后就走掉，没有任何东西留下来，那么**留下来的东西其实就是称之为state**，中文可以翻译成状态。

​		在下面这个图中，所有的原始数据进入用户代码之后再输出到下游，==如果中间涉及到 state 的读写，这些状态会存储在本地的 state backend（可以对标成嵌入式本地 kv 存储）当中==。

![1615166352502](/img/1615166352502.png)

​	

> Flink中状态State，由一个任务SubTask维护，并且用来计算某个结果的所有数据，都属于这个任务的状态。
>
> - 可以认为状态State就是一个本地变量，可以被任务的业务逻辑访问；
> - Flink 会进行状态管理，包括状态一致性、故障处理以及高效存储和访问，以便开发人员可以专注于应用程序的逻辑；
> - 在Flink中，状态始终与特定算子相关联，为了使运行时Flink了解算子的状态，算子需要预先注册其状态

![1630372681268](/img/1630372681268.png)



> 为什么需要State？

- 与批计算相比，==State是流计算特有的==，批计算没有failover机制，要么成功，要么重新计算。
- 流计算在 大多数场景下是==增量计算，数据逐条处理（大多数场景)，每次计算是在上一次计算结果之上进行处理的==，这样的机制势必要[将上一次的计算结果进行存储（生产模式要持久化）]()；
- 另外由于机器、网络、脏数据等原因导致的程序错误，在==重启job时候需要从成功的检查点(checkpoint)进行state的恢复==。增量计算，Failover这些机制都需要state的支撑。





## 05-[掌握]-Flink State之存储 State 数据结构 

> ​		有状态计算其实就是需要==考虑历史数据==，而历史数据需要搞个地方存储起来。Flink为了方便不同分类的State的存储和管理，提供以下保存State的数据结构。

![1630455425242](/img/1630455425242.png)



- `ValueState<T>`：类型为T的[单值]()状态

  - 保存一个可以更新和检索的值（每个值都对应到当前的输入数据的key，因此算子接收到的每个key都可能对应一个值）。

  - 这个值可以通过**update(T)**进行更新，通过**T value()**进行检索。

  ![1630379651613](/img/1630379651613.png)



- `ListState<T>`：key上的状态值为一个[列表]()
  - 保存一个元素的列表，可以往这个列表中追加数据，并在当前的列表上进行检索。
  - 可以通过**add(T)**或者**addAll(List<T>)**进行添加元素，通过**Iterable<T> get()**获得整个列表。
  - 还可以通过**update(List<T>)**覆盖当前的列表。
  - 如统计按用户id统计用户经常登录的IP

![1630379774702](/img/1630379774702.png)



- `MapState<UK,UV>`：即状态值为一个[map]()
  - 维护了一个映射列表，可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。
  - 使用**put(UK，UV)**或者**putAll(Map<UK，UV>)**添加映射。
  - 使用**get(UK)**检索特定key。
  - 使用entries()，keys()和values()分别检索映射、键和值的可迭代视图

![1630379914237](/img/1630379914237.png)



- `ReducingState<T>`：
  - 保存一个单值，表示添加到状态的所有值的聚合。
  - 这种状态通过用户传入的reduceFunction，每次调用**add**方法添加值的时候，会调用**reduceFunction**，最后合并到一个单一的状态值。
  - `AggregatingState<IN,OUT>`：保留一个单值，表示添加到状态的所有值的聚合。和ReducingState相反的是,聚合类型可能与添加到状态的元素的类型不同。
  - `FoldingState<T,ACC>`：保留一个单值，表示添加到状态的所有值的聚合。与ReducingState相反，聚合类型可能与添加到状态的元素类型不同。



- `Broadcast State`：具有Broadcast流的特殊属性
  - 一种小数据状态广播向其它流的形式，从而避免大数据流量的传输;
  - 在这里，其它流是对广播状态只有只读操作的允许，因为不同任务间没有跨任务的信息交流。
  - 一旦有运行实例对于广播状态数据进行更新了，就会造成状态不一致现象。

![1630380220495](/img/1630380220495.png)





## 06-[掌握]-Flink State 类型之基本类型划分 

​		[状态State是指流计算过程中计算节点的中间计算结果或元数据属性]()，比如在aggregation过程中要在state中记录==中间聚合结果==，比如 Apache Kafka 作为数据源时候，也要记录已经==读取记录的offset==，这些State数据在计算过程中会进行持久化(插入或更新)。

> 在Flink中，按照基本类型，对State划分为两类：：==Keyed State 和 Operator State==。

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/state.html

![1626832923841](/img/1626832923841.png)

> 1）、[Keyed State]()：
>
> - 和Key有关的状态类型，KeyedStream流上的==每一个key，都对应一个state==；
>- 只能应用于 KeyedStream 的函数与操作中；
> - keyed state 是已经分区 / 划分好的，每一个 key 只能属于某一个 keyed state；
> - 存储数据结构：**ValueState、ListState、MapState、ReducingState和AggregatingState**等等

​	以WordCount 的 `sum` 所使用的`StreamGroupedReduce`类为例，如何在代码中使用。

![1615171048327](/../03_%E7%AC%94%E8%AE%B0/img/1615171048327.png)

```scala
		// TODO: KeyedState，实现词频统计
		tupleStream
			.keyBy(0) // 分组
			.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
				private ValueState<Long> totalState = null ;

				@Override
				public void open(Configuration parameters) throws Exception {
					totalState = getRuntimeContext().getState(
						new ValueStateDescriptor<Long>("totalState", Long.class)
					) ;
				}

				@Override
				public String map(Tuple2<String, Integer> value) throws Exception {
					// 获取状态中的值 与 新数据的值 相加
					long latestValue = totalState.value() + value.f1;
					// 更新状态中的值
					totalState.update(latestValue);
					// 返回统计的值
					return value.f0 + " = " + latestValue;
				}
			});
```



> 2）、[Operator State]()：
>
> - 又称为 non-keyed state，每一个 operator state 都仅与一个 operator 的实例绑定；
> - 常见的 operator state 是 数据源==source state==，例如记录当前 source 的 offset；
> - 存储数据结构：**ListState或BroadcastState**等等

![1629339202313](/img/1629339202313.png)



> 再看一段使用 OperatorState 的 WordCount 代码：

![1630381446817](/img/1630381446817.png)

此处 `fromElements` 会调用 `FromElementsFunction` 的类，其中就使用了类型为ListState的OperatorState。

```scala
		// TODO: OperatorState，实现计数
		tupleStream.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
			private ListState<Long> counterState = null ;

			@Override
			public void open(Configuration parameters) throws Exception {
				counterState = getRuntimeContext().getListState(
					new ListStateDescriptor<Long>("counterState", Long.class)
				);
			}

			@Override
			public String map(Tuple2<String, Integer> value) throws Exception {
				// 获取状态中的值
				long latestState = counterState.get().iterator().next() + 1L;
				// 更新状态值
				counterState.addAll(Collections.singletonList(latestState));
				// 直接返回数据
				return value.f0 + ", " + value.f1 + " -> " + latestState;
			}
		});
```



> 两种不同类型状态：Keyed State与Operator State，比较如下：

![1615168833112](/img/1615168833112.png)





## 07-[了解]-Flink State 类型之组织形式划分 

> 将状态按照**组织形式**的划分：==**Managed State**和**Raw State**==，可以理解为按照runtime层面的划分

- **Managed State**：管理状态
  - 由Flink运行时控制的数据结构表示，比如内部的hashtable或者RocksDB。
  - 比如ValueState、ListState等，Flinkruntime会对这些状态进行编码并写入checkpoint。
  - *Managed State*可以在所有的DataStream相关方法中被使用，官方也是推荐优先使用这类State，因为它能被Flink runtime内部做自动重分布而且能被更好地进行内存管理。



- **Raw State**：原始状态
  - Raw类型的State则保存在算子自己的数据结构中。
  - checkpoint的时候，Flink并不知晓具体的内容，仅仅写入==一串字节序列==到checkpoint。



> 两种不同组织形式状态：Managed State和Raw State，具体区别如下：

![1615168607962](/img/1615168607962.png)







## 08-[理解]-Flink State 案例之KeyedState

> **KeyedState**是根据输入数据流中定义的键（key）来维护和访问的。
>
> - Flink 为每个 key 维护一个状态实例，并将具有相同键的所有数据，都分区到同一个算子任务中，这个任务会维护和处理这个 key 对应的状态；
> - 当任务处理一条数据时，它会自动将状态的访问范围限定为当前数据的 key

![1630390692997](/img/1630390692997.png)

> 需求：使用KeyState中的`ValueState`获取数据中的**最大值**(实际中直接使用`max`即可)。
>

```scala
DataStreamSource<Tuple3<String, String, Long>> tupleStream = env.fromElements(
    Tuple3.of("上海", "普陀区", 488L), Tuple3.of("上海", "徐汇区", 212L),
    Tuple3.of("北京", "西城区", 823L), Tuple3.of("北京", "海淀区", 234L),
    Tuple3.of("上海", "杨浦区", 888L), Tuple3.of("上海", "浦东新区", 666L),
    Tuple3.of("北京", "东城区", 323L), Tuple3.of("上海", "黄浦区", 111L)
);
```



> 使用KeyedState存储每个Key的最大值，依据案例需求，分析思路如下：

![1629341929502](/img/1629341929502.png)



​			用户自己管理KeyedState，存储Key的状态值，步骤如下：

```scala
// step1、定义状态变量，存储每个单词Key的词频
	private ValueState<Long> valueState = null ;

// step2、初始化状态变量，通过状态描述符
	valueState = getRuntimeContext().getState(
        new ValueStateDescriptor<Long>("maxState", Long.class)
    );

// step3、对Key相同新数据处理时，从状态中获取Key以前词频
	Long historyValue = valueState.value();

// step4、数据处理并输出后，更新状态中的值
	valueState.update(currentValue);
```



​		编写代码，基于KeyedState状态实现获取最大值max函数功能，具体如下：

```scala
package cn.itcast.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink State 中KeyedState，默认情况下框架自己维护，此外可以手动维护
 */
public class StreamKeyedStateDemo {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<Tuple3<String, String, Long>> tupleStream = env.fromElements(
			Tuple3.of("上海", "普陀区", 488L), Tuple3.of("上海", "徐汇区", 212L),
			Tuple3.of("北京", "西城区", 823L), Tuple3.of("北京", "海淀区", 234L),
			Tuple3.of("上海", "杨浦区", 888L), Tuple3.of("上海", "浦东新区", 666L),
			Tuple3.of("北京", "东城区", 323L), Tuple3.of("上海", "黄浦区", 111L)
		);

		// 3. 数据转换-transformation
		// TODO：使用DataStream转换函数max获取每个市最大值
		SingleOutputStreamOperator<Tuple3<String, String, Long>> maxDataStream = tupleStream
			.keyBy(0)
			.max(2);
		// 4. 数据终端-sink
		/*
			(上海,普陀区,488)
			(上海,普陀区,488)
			(北京,西城区,823)
			(北京,西城区,823)
			(上海,杨浦区,888)
			(上海,杨浦区,888)
			(北京,西城区,823)
			(上海,杨浦区,888)
		 */
		//maxDataStream.printToErr();

		// TODO: 自己管理KededState存储每个Key状态State
		SingleOutputStreamOperator<String> stateDataStream = tupleStream
			.keyBy(0) // 城市city分组
			.map(new RichMapFunction<Tuple3<String, String, Long>, String>() {
				// TODO: step1. 定义存储转态数据结构
				// transient是类型修饰符，只能用来修饰字段。在对象序列化的过程中，标记为transient的变量不会被序列化
				private ValueState<Long> valueState = null ;

				@Override
				public void open(Configuration parameters) throws Exception {
					// TODO: step2. 初始化状态的值
					valueState = getRuntimeContext().getState(
						new ValueStateDescriptor<Long>("maxState", Long.class)
					);
				}

				@Override
				public String map(Tuple3<String, String, Long> tuple) throws Exception {
					/*
						Key: 上海，  tuple： 上海,普陀区,488
					 */
					// TODO: step3. 从以前状态中获取值
					Long historyValue = valueState.value();
					// 获取当前key中的值
					Long currentValue = tuple.f2 ;
					// 判断历史值是否为null，如果key的数据第1次出现，以前没有状态
					if(null == historyValue || currentValue > historyValue){
						// TODO: step4. 更新状态值
						valueState.update(currentValue);
					}
					// 返回处理结果
					return tuple.f0 + ", " + valueState.value();
				}
			});
		/*
			上海, 488
			上海, 488
			北京, 823
			北京, 823
			上海, 888
			上海, 888
			北京, 823
			上海, 888
		 */
		stateDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamKeyedStateDemo") ;
	}

}
```





## 09-[了解]-Flink State 案例之Operator State 

> **OperatorState** 状态针对非分组keyBy数据流DataStream状态管理，常常应用于数据源Source。
>
> - 算子状态的作用范围限定为算子任务，由同一并行任务所处理的所有数据都可以访问到相同的状态；
> - 状态对于同一子任务而言是共享的；
> - Kafka Connectors连接器中，提供的从Kafka消费数据：`FlinkKafkaConsumer`，实现自己管理状态：OperatorState，使用数据结构为ListState（列表存储状态）。

![1630395025776](/img/1630395025776.png)

​		

​		**模拟从Kafka消费数据，自己管理消费偏移量，此时OperatorState操作，使用`ListState`存储状态，并且要求数据源实现`CheckpointFunction`接口，将状态进行Checkpoint操作，以便容灾恢复。**

![1630395435350](/img/1630395435350.png)



> 需求：使用ListState存储offset，模拟Kafka的offset维护，重启流式应用从Checkpoint恢复上次消费offset。

![1630397140273](/img/1630397140273.png)

> 将状态State进行快照SnapShot并保存（Checkpoint），实现接口：`CheckpointedFunction`。

![1630409256708](/img/1630409256708.png)

- 状态快照方法`snapshotState`：将某时刻State状态进行快照，并保存到外部存储，比如HDFS文件系统；
- 初始化状态`initializeState`：流式应用重启时，从Checkpoint检查点进行恢复状态；



> ​	编写代码，基于OperatorState状态实现获取从Kafka消费数据时偏移量保存及从检查点恢复，具体如下：

```java
package cn.itcast.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Flink State 中OperatorState，自定义数据源Kafka消费数据，保存消费偏移量数据并进行Checkpoint
 */
public class StreamOperatorStateDemo {

	/**
	 * 自定义数据源，模拟从Kafka消费数据，管理消费偏移量，进行状态存储
	 */
	private static class KafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
		// 标识程序是否运行
		private boolean isRunning = true ;

		// TODO: step1. 定义存储偏移量状态
		private transient ListState<Long> offsetState = null ;
		// 定义偏移量
		private Long offset = 0L ;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning){
				// 模拟从Kafka消费数据
				int partitionId = getRuntimeContext().getIndexOfThisSubtask();
				offset += 1 ;
				// 输出数据
				ctx.collect("p-" + partitionId + ": " + offset);

				// TODO: step 3. 更新偏移量到状态中
				offsetState.update(Collections.singletonList(offset));

				// 每隔1秒消费数据
				TimeUnit.SECONDS.sleep(1);
				// 当偏移量被5整除时，抛出异常
				if(offset % 5 == 0){
					throw new RuntimeException("程序处理异常啦啦啦啦.................") ;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}

		// 流式启动时，初始化状态，如果不是第一次从Checkpoint获取值
		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			// TODO: step2. 状态初始化
			// 状态描述符
			ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("offsetState", Long.class);
			// 初始化State对象实例
			offsetState = context.getOperatorStateStore().getListState(descriptor);

			// TODO: 如果从检查点恢复，获取State状态中的值
			if(context.isRestored()){
				offset = offsetState.get().iterator().next();
			}
		}

		// 将State状态进行快照保存
		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			// 状态保存至外部存储
			offsetState.clear();
			// 更新最新状态值，以便下次进行保存
			offsetState.update(Collections.singletonList(offset));
		}
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置并行度为3
		env.setParallelism(3) ;
		// TODO: 设置检查点Checkpoint相关属性，保存状态和应用重启策略
		setEnvCheckpoint(env) ;

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.addSource(new KafkaSource());

		// 3. 数据转换-transformation
		// 4. 数据终端-sink
		inputDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamOperatorStateDemo") ;
	}

	/**
	 * 设置Flink Stream流式应用Checkpoint相关属性
	 */
	private static void setEnvCheckpoint(StreamExecutionEnvironment env){
		// 每隔1s执行一次Checkpoint
		env.enableCheckpointing(1000) ;
		// 状态数据保存本地文件系统
		env.setStateBackend(new FsStateBackend("file:///D:/ckpt/")) ;
		// 当应用取消时，Checkpoint数据保存，不删除
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);
		// 设置模式Mode为精确性一次语义
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// 固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
	}

}
```

> 程序异常以后，自动从检查点目录恢复，代码设置Checkpoint相关属性，内容如下：

```Java
private static void setEnvCheckpoint(StreamExecutionEnvironment env){
    // 每隔1s执行一次Checkpoint
    env.enableCheckpointing(1000) ;
    // 状态数据保存本地文件系统
    env.setStateBackend(new FsStateBackend("file:///D:/ckpt/")) ;
    // 当应用取消时，Checkpoint数据保存，不删除
    env.getCheckpointConfig().enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    );
    // 设置模式Mode为精确性一次语义
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    // TODO: 当不设置重启策略时，将会无限重启
}
```





## 10-[理解]-Flink Checkpoint之State与Checkpoint 

> ​			什么是Checkpoint？也就是所谓的==检查点==，是用来故障恢复用的一种机制。Spark也有Checkpoint，Flink与Spark一样，都是**用Checkpoint来存储某一时间或者某一段时间的快照（snapshot），用于将任务恢复到指定的状态。**
>

![1615175537770](/img/1615175537770.png)

状态State与检查点Checkpoint之间关系：`Checkpoint将某个时刻应用状态State进行快照Snapshot保存`。



> - 1）、`State`：维护/存储的是某一个Operator的运行的状态/历史值，是**维护在内存**中。

![1615175704827](/img/1615175704827.png)



> - 2）、`Checkpoint`：某一时刻，Flink中所有的Operator的当前**State的全局快照**，一般存在**磁盘上**。

![1615175769512](/img/1615175769512.png)



> ​			Flink的Checkpoint的核心算法叫做`Chandy-Lamport`，是一种分布式快照（Distributed Snapshot）算法，应用到流式系统中就是确定一个 Global 的 Snapshot，错误处理的时候各个节点根据上一次的 Global Snapshot 来恢复。

分布式快照算法: Chandy-Lamport 算法：https://zhuanlan.zhihu.com/p/53482103

![Chandy-Lamport算法核心解读](/img/v2-4e5916b1a0bd335e22c39c820692c8d9_1440w.jpg)





## 11-[掌握]-Flink Checkpoint之执行流程（简易版） 

​		Checkpoint是Flink实现容错机制最核心的功能，它能够根据配置[周期性地基于Stream中各个Operator/task的状态State来生成快照，从而将这些状态数据定期持久化存储下来，当Flink程序一旦意外崩溃时，重新运行程序时可以有选择地从这些快照进行恢复，从而修正因为故障带来的程序数据异常]()。

> ​		Checkpoint实现的核心就是`barrier（栅栏或屏障）`，Flink通过在数据集上**间隔性**的生成**屏障barrier**，并通过barrier将某段时间内的数据保存到Checkpoint中（先快照，再保存）。
>
> - barrier被注入数据流并与记录一起作为数据流的一部分向下流动
> - barrier将数据流中的记录隔离成一系列的记录集合，并将一些集合中的数据加入到当前的快照中，而另一些数据加入到下一个快照中
> - 每个barrier都带有快照的ID，并且barrier之前的记录都进入了该快照，
> - barriers不会中断流处理，非常轻量级。

![img](/img/20160721153249897)



> 如下图展示Checkpoint时整体流程，简易版本：

![1615175927939](/img/1615175927939.png)

- 1）、Flink的`JobManager`创建`CheckpointCoordinator`；
- 2）、Coordinator向所有的`SourceOperator`发送Barrier栅栏(理解为执行Checkpoint的信号)；
- 3）、SourceOperator接收到Barrier之后，暂停当前的操作(暂停的时间很短，因为后续的写快照是异步的)，并制作State快照, 然后将自己的快照保存到指定的介质中(如HDFS), 一切 ok之后向Coordinator汇报并将Barrier发送给下游的其他Operator；
- 4）、其他的如TransformationOperator接收到Barrier，重复第2步，最后将Barrier发送给Sink；
- 5）、Sink接收到Barrier之后重复第2步；
- 6）、Coordinator接收到所有的Operator的执行ok的汇报结果，认为本次快照执行成功；

![1615176327301](/img/1615176327301.png)



> ==栅栏对齐==：下游的subTask必须接收到上游的**所有SubTask**发送Barrier栅栏信号，才开始进行Checkpoint操作。

![1630417416602](/img/1630417416602.png)





## 12-[了解]-Flink Checkpoint之执行流程（详细版） 

Flink Checkpoint时详细版执行流程如下：

- 下图左侧是 Checkpoint Coordinator，是整个 Checkpoint的发起者；
- 中间是由两个 source，一个 sink 组成的Flink作业；
- 最右侧的是持久化存储，在大部分用户场景中对应 HDFS

> 1）、Checkpoint Coordinator 向所有 source 节点 trigger Checkpoint（[触发Checkpoint，发送Barrier栅栏]()）；

![1615176703873](/img/1615176703873.png)



> 2）、广播barrier并进行持久化
>
> - ​	source 节点向下游广播 barrier，这个 barrier 就是实现 Chandy-Lamport 分布式快照算法的核心，下游的 task `只有收到所有 input 的 barrier` 才会执行相应的 Checkpoint。
> - Source将状态State进行快照，并且进行持久化到存储系统。

![1615176765255](/img/1615176765255.png)





> - 3）、当task完成state备份后，会将`备份数据的地址（state handle）通知给 Checkpointcoordinator`

![1615176867045](/img/1615176867045.png)





> 4）、下游的 sink 节点收集齐上游两个 input 的 barrier 之后（栅栏对齐），将执行本地快照。
>
> - 先写入红色的三角形（RocksDB），在写入第三方持久化数据库中（紫色三角形）。
> - 展示了 `RocksDB` incremental Checkpoint (`增量Checkpoint`)的流程，首先 RocksDB 会全量刷数据到磁盘上（红色大三角表示），然后 Flink 框架会从中选择没有上传的文件进行持久化备份（紫色小三角）。

![1615176931696](/img/1615176931696.png)





> 5）、同样的，sink 节点在完成自己的 Checkpoint 之后，会将 state handle 返回通知Coordinator。

![1615177012225](/img/1615177012225.png)





> 6）、最后，当 Checkpoint coordinator 收集齐所有 task 的 state handle，就认为这一次的Checkpoint 全局完成了，向持久化存储中再备份一个 Checkpoint meta 文件。

![1615177057850](/img/1615177057850.png)





## 13-[掌握]-Flink Checkpoint之StateBackend 

> ​		Checkpoint其实就`是Flink中某一时刻，所有的Operator的全局快照，那么快照应该要有一个地方进行存储`，而这个存储的地方叫做**状态后端（StateBackend**）。

![1615177274852](/img/1615177274852.png)

> - 1）、`MemStateBackend`
>    - State存储：**TaskManager**内存中
>     - Checkpoint存储：**JobManager**内存中
> 

[推荐使用的场景为：本地测试、几乎无状态的作业，比如 ETL、JobManager 不容易挂，或挂掉影响不大的情况。不推荐在生产场景使用。]()

![1615177332962](/img/1615177332962.png)



> - 2）、`FsStateBackend`
>    - State存储：==TaskManager==内存
>     - Checkpoint存储：可靠外部存储文件系统，本地测试可以为LocalFS，==测试生产HDFS==
> 

[推荐使用的场景为：常规使用状态的作业，例如分钟级窗口聚合或 join、需要开启HA的作业]()

![1615177445604](/img/1615177445604.png)

> 当Checkpoint时存储到文件系统时，设置格式

![1615177536558](/img/1615177536558.png)



> - 3）、`RocksDBStateBackend`
>    - RocksDB 是一个 key/value 的内存存储系统，和其他的 key/value 一样，==先将状态放到内存中，如果内存快满时，则写入到磁盘中==。类似Redis内存数据库。
>     - State存储：TaskManager内存数据库（==RocksDB==）
>     - Checkpoint存储：外部文件系统，比如HDFS可靠文件系统中
> 

[推荐使用的场景为：超大状态的作业，例如天级窗口聚合、需要开启 HA 的作业、最好是对状态读写性能要求不高的作业。]()

![1615177612378](/img/1615177612378.png)



> 总述，Flink StateBackend状态后端实现类：

![1615177792397](/img/1615177792397.png)





## 14-[了解]-Flink Checkpoint之Checkpoint 配置方式 

在Flink如何配置Checkpoint，有如下几种方式：

> - 1）、全局配置，配置文件：`flink-conf.yaml`

![1615185513004](/img/1615185513004.png)



> - 2）、在代码中配置：每个应用单独配置

![1630419273367](/img/1630419273367.png)

注意：如果将State存储`RocksDBStateBackend`内存中，需要引入相关依赖

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
    <version>1.10.0</version>
</dependency>
```





## 15-[掌握]-Flink Checkpoint之Checkpoint 案例演示 

> ​			编写Flink入门案例程序，词频统计WordCount，**自定义数据源**，产生数据：`spark flink`，设置Checkpoint，运行程序，查看Checkpoint检查点数据存储。

```Java
// TODO： ================= 建议必须设置 ===================
// a. 设置Checkpoint-State的状态后端为FsStateBackend，本地测试时使用本地路径，集群测试时使用传入的HDFS的路径
if (args.length < 1) {
	env.setStateBackend(new FsStateBackend("file:///D:/datas/ckpt"));
	//env.setStateBackend(new FsStateBackend("hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint"));
} else {
	// 后续集群测试时，传入参数：hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint
	env.setStateBackend(new FsStateBackend(args[0]));
}
/*
b. 设置Checkpoint时间间隔为1000ms，意思是做 2 个 Checkpoint 的间隔为1000ms。
Checkpoint 做的越频繁，恢复数据时就越简单，同时 Checkpoint 相应的也会有一些IO消耗。
*/
env.enableCheckpointing(1000);// 默认情况下如果不设置时间checkpoint是没有开启的
/*
c. 设置两个Checkpoint 之间最少等待时间，如设置Checkpoint之间最少是要等 500ms
为了避免每隔1000ms做一次Checkpoint的时候，前一次太慢和后一次重叠到一起去了
如:高速公路上，每隔1s关口放行一辆车，但是规定了两车之前的最小车距为500m
*/
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// d. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是 false不是
env.getCheckpointConfig().setFailOnCheckpointingErrors(false); // 默认为true
// 设置Checkpoint时失败次数，允许失败几次
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); //

/*
e. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false，当作业被取消时，保留外部的checkpoint
ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
*/
env.getCheckpointConfig().enableExternalizedCheckpoints(
	CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);

// ================= 直接使用默认的即可 ===============
// a. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意:需要外部支持，如Source和Sink的支持
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// b. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
env.getCheckpointConfig().setCheckpointTimeout(60000);
// c. 设置同一时间有多少个checkpoint可以同时执行
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 默认为1

// ===========================================================================================
```



> 代码中加上上述针对Checkpoint设置代码，完整代码如下：

![1630420070615](/img/1630420070615.png)

```scala
package cn.itcast.flink.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Flink Checkpoint定时保存状态State，演示案例
 */
public class _04StreamCheckpointDemo {

	/**
	 * 自定义数据源，每隔一定时间产生字符串：
	 */
	private static class MySource extends RichParallelSourceFunction<String> {
		private boolean isRunning = true ;
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning){
				// 发送数据
				ctx.collect("spark flink");

				// 每隔1秒产生一条数据
				TimeUnit.SECONDS.sleep(1);
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}
	}

	/**
	 * Flink Stream流式应用，Checkpoint检查点属性设置
	 */
	private static void setEnvCheckpoint(StreamExecutionEnvironment env, String[] args) {
		/* TODO： ================================== 建议必须设置 ================================== */
// a. 设置Checkpoint-State的状态后端为FsStateBackend，本地测试时使用本地路径，集群测试时使用传入的HDFS的路径
		if (args.length < 1) {
			env.setStateBackend(new FsStateBackend("file:///D:/datas/ckpt"));
			//env.setStateBackend(new FsStateBackend("hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint"));
		} else {
			// 后续集群测试时，传入参数：hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint
			env.setStateBackend(new FsStateBackend(args[0]));
		}
/*
b. 设置Checkpoint时间间隔为1000ms，意思是做 2 个 Checkpoint 的间隔为1000ms。
Checkpoint 做的越频繁，恢复数据时就越简单，同时 Checkpoint 相应的也会有一些IO消耗。
*/
		env.enableCheckpointing(1000);// 默认情况下如果不设置时间checkpoint是没有开启的
/*
c. 设置两个Checkpoint 之间最少等待时间，如设置Checkpoint之间最少是要等 500ms
为了避免每隔1000ms做一次Checkpoint的时候，前一次太慢和后一次重叠到一起去了
如:高速公路上，每隔1s关口放行一辆车，但是规定了两车之前的最小车距为500m
*/
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// d. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是 false不是
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false); // 默认为true
// 设置Checkpoint时失败次数，允许失败几次
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); //

/*
e. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false，当作业被取消时，保留外部的checkpoint
ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
*/
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		/* TODO： ================================== 直接使用默认的即可 ================================== */
// a. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意:需要外部支持，如Source和Sink的支持
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// b. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
		env.getCheckpointConfig().setCheckpointTimeout(60000);
// c. 设置同一时间有多少个checkpoint可以同时执行
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 默认为1
	}

	/**
	 * 数据转换，调用函数，实现词频统计WordCount
	 */
	private static DataStream<Tuple2<String, Integer>> processStream(DataStream<String> datastream) {
		// 调用转换函数，处理数据
		SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = datastream
			.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public void flatMap(String value, Collector<String> out) throws Exception {
					for (String word : value.split("\\s+")) {
						out.collect(word);
					}
				}
			})
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String value) throws Exception {
					return Tuple2.of(value, 1);
				}
			})
			.keyBy(0).sum(1);
		// 返回结果
		return resultDataStream;
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO： 检查点Checkpoint设置
		setEnvCheckpoint(env, args);

		// 2. 数据源-source
		DataStream<String> inputDataStream = env.addSource(new MySource());

		// 3. 数据转换-transformation
		DataStream<Tuple2<String, Integer>> resultDataStream = processStream(inputDataStream) ;

		// 4. 数据终端-sink
		resultDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_04StreamCheckpointDemo.class.getSimpleName()) ;
	}

}
```





## 16-[掌握]-Flink Checkpoint之手动重启及状态恢复 

在Flink流式计算程序中，如果设置Checkpoint检查点以后，当应用程序运行失败，可以从检查点恢复：

> - 1）、`手动重启`应用，从Checkpoint恢复状态
>    - 程序升级（人为停止程序）等
> - 2）、`自动重启`应用，从Checkpoint恢复状态
>   - 程序异常时，自动重启，继续运行处理数据，比如出现“脏数据”
>   - 自动重启，设置最大重启次数，如果重启超过设置次数，需要人为干预，进行手动重启
> 

[将上述Flink程序，打成jar包，在Flink Standalone Cluster上提交运行]()，具体操作步骤如下所示：

- step1、把程序打包

![1615186916027](/img/1615186916027.png)



- step2、启动Flink集群(本地单机版，集群版都可以)

  ```ini
  [root@node1 ~]# /export/server/flink/bin/start-cluster.sh
  Starting cluster.
  Starting standalonesession daemon on host node1.itcast.cn.
  Starting taskexecutor daemon on host node1.itcast.cn.
  
  # 如果HDFS没有启动，先启动服务，将Checkpoint数据保存到HDFS文件系统
  [root@node1 ~]# hadoop-daemon.sh start namenode 
  starting namenode, logging to /export/server/hadoop-2.7.5/logs/hadoop-root-namenode-node1.itcast.cn.out
  
  [root@node1 ~]# 
  [root@node1 ~]# hadoop-daemon.sh start datanode 
  starting datanode, logging to /export/server/hadoop-2.7.5/logs/hadoop-root-datanode-node1.itcast.cn.out
  ```



- step3、访问webUI：http://node1.itcast.cn:8081/#/overview

![1615187000536](/img/1615187000536.png)



- step4、使用Flink WebUI提交，填写如下参数

```ini
cn.itcast.flink.checkpoint.StreamCheckpointDemo
hdfs://node1.itcast.cn:8020/flink-checkpoint/checkpoint
```

![1615187075153](/img/1615187075153.png)



- step5、取消任务

![1615187191533](/img/1615187191533.png)



​			查看HDFS目录，Checkpoint存文件

![1615187258934](/img/1615187258934.png)



- step6、重新启动任务并指定从哪恢复

```ini
cn.itcast.flink.checkpoint.StreamCheckpointDemo
hdfs://node1.itcast.cn:8020/flink-checkpoint/checkpoint
hdfs://node1.itcast.cn:8020/flink-checkpoint/checkpoint/de1d2ddd997a0627240500608c025f1d/chk-59
```

![1615187352004](/img/1615187352004.png)



> 使用`flink run` 运行Job执行，指定参数选项 `-s path`，从Checkpoint检查点启动，恢复以前状态。

![1626850363768](/img/1626850363768.png)





## 17-[理解]-Flink Checkpoint之自动重启策略【配置】

> 在Flink流式计算程序中，可以设置当应用处理数据异常时，可以自动重启，相关设置如下：
>
> - 1）、==重启策略配置方式==

![1615187573808](/img/1615187573808.png)



> 重启策略分为4类：
>
> - 1）、`默认重启策略`
>   - 如果配置Checkpoint，没有配置重启策略，那么代码中出现了非致命错误时，程序会无限重启。
> - 2）、`无重启策略`
>   - Job直接失败，不会尝试进行重启

![1615187799326](/img/1615187799326.png)



> - 3）、==固定延迟重启策略==（开发中使用）
>   - 设置固定重启次数，及重启间隔时间

![1615187846336](/img/1615187846336.png)



> - 4）、==失败率重启策略==（偶尔使用）

![1630421201833](/img/1630421201833.png)





## 18-[掌握]-Flink Checkpoint之自动重启策略【案例】

> 修改前面Checkpoint程序，设置自动重启策略：自动重启3次，每次时间间隔为5秒。

![1630421256153](/img/1630421256153.png)

```scala
package cn.itcast.flink.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Flink Checkpoint定时保存状态State，演示案例
 */
public class _05StreamRestartStrategyDemo {

	/**
	 * 自定义数据源，每隔一定时间产生字符串：
	 */
	private static class MySource extends RichParallelSourceFunction<String> {
		private boolean isRunning = true ;
		private int counter = 0 ;
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (isRunning){
				// 发送数据
				ctx.collect("spark flink");
				counter += 1 ;

				// 每隔1秒产生一条数据
				TimeUnit.MILLISECONDS.sleep(1000);

				if(counter % 5 == 0){
					throw new RuntimeException("程序异常啦啦啦啦啦.................") ;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}
	}

	/**
	 * Flink Stream流式应用，Checkpoint检查点属性设置
	 */
	private static void setEnvCheckpoint(StreamExecutionEnvironment env, String[] args) {
		/* TODO： ================================== 建议必须设置 ================================== */
// a. 设置Checkpoint-State的状态后端为FsStateBackend，本地测试时使用本地路径，集群测试时使用传入的HDFS的路径
		if (args.length < 1) {
			env.setStateBackend(new FsStateBackend("file:///D:/datas/ckpt"));
			//env.setStateBackend(new FsStateBackend("hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint"));
		} else {
			// 后续集群测试时，传入参数：hdfs://node1.itcast.cn:8020/flink-checkpoints/checkpoint
			env.setStateBackend(new FsStateBackend(args[0]));
		}
/*
b. 设置Checkpoint时间间隔为1000ms，意思是做 2 个 Checkpoint 的间隔为1000ms。
Checkpoint 做的越频繁，恢复数据时就越简单，同时 Checkpoint 相应的也会有一些IO消耗。
*/
		env.enableCheckpointing(1000);// 默认情况下如果不设置时间checkpoint是没有开启的
/*
c. 设置两个Checkpoint 之间最少等待时间，如设置Checkpoint之间最少是要等 500ms
为了避免每隔1000ms做一次Checkpoint的时候，前一次太慢和后一次重叠到一起去了
如:高速公路上，每隔1s关口放行一辆车，但是规定了两车之前的最小车距为500m
*/
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// d. 设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是 false不是
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false); // 默认为true
// 设置Checkpoint时失败次数，允许失败几次
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3); //

/*
e. 设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false，当作业被取消时，保留外部的checkpoint
ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
*/
		env.getCheckpointConfig().enableExternalizedCheckpoints(
			CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		/* TODO： ================================== 直接使用默认的即可 ================================== */
// a. 设置checkpoint的执行模式为EXACTLY_ONCE(默认)，注意:需要外部支持，如Source和Sink的支持
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// b. 设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
		env.getCheckpointConfig().setCheckpointTimeout(60000);
// c. 设置同一时间有多少个checkpoint可以同时执行
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 默认为1
	}

	/**
	 * 数据转换，调用函数，实现词频统计WordCount
	 */
	private static DataStream<Tuple2<String, Integer>> processStream(DataStream<String> datastream) {
		// 调用转换函数，处理数据
		SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = datastream
			.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public void flatMap(String value, Collector<String> out) throws Exception {
					for (String word : value.split("\\s+")) {
						out.collect(word);
					}
				}
			})
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String value) throws Exception {
					return Tuple2.of(value, 1);
				}
			})
			.keyBy(0).sum(1);
		// 返回结果
		return resultDataStream;
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;
		// TODO： 检查点Checkpoint设置
		setEnvCheckpoint(env, args);
		// TODO：设置应用程序重启策略
		env.setRestartStrategy(
			// 重启3次，时间间隔为5s
			RestartStrategies.fixedDelayRestart(3, 5000)
		);

		// 2. 数据源-source
		DataStream<String> inputDataStream = env.addSource(new MySource());

		// 3. 数据转换-transformation
		DataStream<Tuple2<String, Integer>> resultDataStream = processStream(inputDataStream) ;

		// 4. 数据终端-sink
		resultDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute("StreamRestartStrategyDemo") ;
	}

}
```





## 19-[理解]-Flink Checkpoint之Savepoint 概述 

​			Flink流式计算，提供**Checkpoint机制，程序自动将State进行快速Snapshot，然后进行Checkpoint保存**。此外，还支持用户可以==手动进行Snapshot==，保存State数据，称为：`SavePoint`保存点。

> [SavePoint保存点由==用户==手动创建、拥有和删除，它们的用例用于有计划的、手动的备份和恢复。]()

![1615190134655](/img/1615190134655.png)



> `Savepoint`：保存点，类似于以前玩游戏的时候，遇到难关/遇到boss，赶紧手动存个档，然后接着玩，如果失败了，赶紧从上次的存档中恢复，然后接着玩。

![1615190278184](/img/1615190278184.png)



> **保存点SavePoint和检查点Checkpoint**区别：

![1630378419852](/img/1630378419852.png)



> 保存点Savepoint，官方提供命令；

```ini
Action "savepoint" triggers savepoints for a running job or disposes existing ones.

  Syntax: savepoint [OPTIONS] <Job ID> [<target directory>]
  "savepoint" action options:
     -d,--dispose <arg>       Path of savepoint to dispose.
     -j,--jarfile <jarfile>   Flink program JAR file.
  Options for yarn-cluster mode:
     -m,--jobmanager <arg>            Address of the JobManager (master) to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -yid,--yarnapplicationId <arg>   Attach to running YARN session
     -z,--zookeeperNamespace <arg>    Namespace to create the Zookeeper
                                      sub-paths for high availability mode

  Options for executor mode:
     -D <property=value>   Generic configuration options for
                           execution/deployment and for the configured executor.
                           The available options can be found at
                           https://ci.apache.org/projects/flink/flink-docs-stabl
                           e/ops/config.html
     -e,--executor <arg>   The name of the executor to be used for executing the
                           given job, which is equivalent to the
                           "execution.target" config option. The currently
                           available executors are: "remote", "local",
                           "kubernetes-session", "yarn-per-job", "yarn-session".

  Options for default mode:
     -m,--jobmanager <arg>           Address of the JobManager (master) to which
                                     to connect. Use this flag to connect to a
                                     different JobManager than the one specified
                                     in the configuration.
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths
                                     for high availability mode
```





## 20-[掌握]-Flink Checkpoint之Savepoint 演示 

**案例：**运行前面Checkpoint程序（配置自动重启策略），将其运行在YARN集群上，采用==WEB UI界面方式部署==运行（flink on yarn ==Session会话==模式）。

```ini
# 1）、启动HDFS集群和YARN集群
[root@node1 ~]# jps
13488 NodeManager
13637 Jps
7464 DataNode
7369 NameNode
13243 ResourceManager

# 2）、启动yarn session
/export/server/flink/bin/yarn-session.sh -jm 1024 -tm 1024 -s 2 -d


# 3）、运行job-会自动执行Checkpoint
/export/server/flink/bin/flink run -d \
--class cn.itcast.flink.checkpoint.StreamCheckpointDemo \
/root/ckpt.jar hdfs://node1.itcast.cn:8020/flink-checkpoint/checkpoint


# 4）、手动创建savepoint--相当于手动做了一次Checkpoint
/export/server/flink/bin/flink savepoint 9d341c4cca156abe4ff0d10f00ce3727 \
hdfs://node1.itcast.cn:8020/flink-checkpoint/savepoint/


#5）、 停止job
/export/server/flink/bin/flink cancel 9d341c4cca156abe4ff0d10f00ce3727


# 6）、重新启动job,手动加载savepoint数据
/export/server/flink/bin/flink run -d \
-s hdfs://node1.itcast.cn:8020/flink-checkpoint/savepoint/savepoint-9d341c-62ee028448d8 \
--class cn.itcast.flink.checkpoint.StreamCheckpointDemo \
/root/ckpt.jar hdfs://node1.itcast.cn:8020/flink-checkpoint/checkpoint


#7）、停止YARN应用
 yarn application -kill application_1615190736728_0001
```



> ​		对Flink Job应用进行SavePoint时，应该==确保状态不变性==，比如先将业务数据停止存储到Kafka 消息队列中，再去对Flink Job进行SavePoint操作，最后取消/停止Job执行。
>





## 21-[了解]-End-to-End Exactly-Once之流计算一致性语义 

> ​		对于流处理器内部来说，所谓的**状态一致性**，其实就是所说的==计算结果要保证准确==。 [一条数据不应该丢失，也不应该重复计算，在遇到故障时可以恢复状态，恢复以后的重新计算，结果应该也是完全正确的。]()

- 有状态的流处理，内部每个算子任务都有自己的状态
- 对于流处理器内部来说，所谓的状态一致性，其实就是我们所说的计算结果要保证准确
- 一条数据不用丢失，也不应该重复计算
- 在遇到故障时可以恢复状态，恢复以后的重新计算，结果也是完全正确的

![1615191889986](img/1615191889986.png)

​		Flink的检查点和恢复机制定期的会保存应用程序状态的一致性检查点。在故障的情况下，应用程序的状态将会从最近一次完成的检查点恢复，并继续处理。尽管如此，可以使用检查点来重置应用程序的状态无法完全达到令人满意的一致性保证。相反，**source和sink的连接器需要和Flink的检查点和恢复机制进行集成才能提供有意义的一致性保证。**

> ​		流处理引擎通常为应用程序提供了三种数据处理语义：==最多一次、至少一次和精确一次==，不同处理语义的宽松定义(一致性由弱到强)：

![1615192082870](/img/1615192082870.png)



> - 1）、最多一次：`At-most-once`：数据可能丢失，没有进行处理

​		当任务故障时，最简单的做法是什么都不干，==既不恢复丢失的状态，也不重播丢失的数据==。At-most-once 语义的含义是最多处理一次事件。

![1630423285765](/img/1630423285765.png)





> - 2）、至少一次：`At-least-once`，数据可能被处理多次

​		在大多数的真实应用场景，希望不丢失事件。这种类型的保障称为 at-least-once，意思是==所有的事件都得到了处理，而一些事件还可能被处理多次==。

![1630423366007](/img/1630423366007.png)





> - 3）、精确一次：`Exactly-once`，数据被处理一次，不会丢弃，也不会重复
>

 			恰好处理一次是最严格的保证，也是最难实现的。恰好处理一次语义不仅仅意味着没有事件丢失，还意味着==针对每一个数据，内部状态仅仅更新一次==。

![1630423431749](/img/1630423431749.png)

​		Flink的 checkpoint机制和故障恢复机制给Flink内部提供了精确一次的保证，需要注意的是，所谓精确一次并不是说精确到每个event只执行一次，而是**每个event对状态（计算结果）的影响只有一次**。





> - 4）、`End-to-End Exactly-Once`
>
> [Flink 在1.4.0 版本引入『exactly-once』并号称支持『End-to-End Exactly-Once』“端到端的精确一次”语义。]()

- 端到端的一致性保证意味着结果的正确性贯穿了整个流处理应用的始终，每一个组件都保证了它自己的一致性
- 端到端的一致性保证指的是 ==Flink 应用从 Source 端开始到 Sink 端结束，数据必须经过的起始点和结束点==。

![1630423670350](/img/1630423670350.png)





> 『exactly-once』和『End-to-End Exactly-Once』的区别，如下图所示：

![1630423917251](/img/1630423917251.png)

![1630423927989](/img/1630423927989.png)





## 22-[理解]-End-to-End Exactly-Once之一致性语义实现

​		在流式计算引擎中，如果要实现精确一致性语义，有如下三种方式：

> - 1）、方式一：**至少一次+去重**

![1615192697308](/img/1615192697308.png)



> - 2）、方式二：**至少一次+幂等**

![1615192717064](/img/1615192717064.png)



> - 3）、方式三：**分布式快照**

![1615192787823](/img/1615192787823.png)



> 上述三种实现流式计算一致性语义方式，综合相比较，如下图所示：

![1630424060511](/img/1630424060511.png)





## 23-[掌握]-End-to-End Exactly-Once之Flink 一致性实现

​				Flink 内部借助`分布式快照`Checkpoint已经实现了内部的Exactly-Once，但是Flink自身是无法保证外部其他系统“精确一次”语义的，所以**Flink 若要实现所谓“端到端（End to End）的精确一次”的要求，那么外部系统必须支持“精确一次”语义，然后借助一些其他手段才能实现**。



> StructuredStreaming 流式应用程序精确一次性语义实现，三个方面要求：

- **数据源Source**：支持偏移量，比如Kafka支持；
- **数据转换Transformatio**n：Checkpoint和WAL预写日志；
- **数据终端Sink**：支持幂等性

![1615194217019](/img/1615194217019.png)



> - 2）、Flink 流式应用程序精确一次性语义实现

- **数据源Source**：支持`重设数据的读取位置`，比如偏移量offfset（kafka消费数据）
- **数据转换Transformation**：Checkpoint检查点机制（采用分布式快照算法实现一致性）
- **数据终端Sink**：要么支持==幂等性写入==，要么==事务写入结合Checkpoint==。

![1615194321143](/img/1615194321143.png)



​		在Flink中Sink要实现精确一次性：

> - 1）、`幂等写入（Idempotent Writes）`

![1615194529754](/img/1615194529754.png)



> - 2）、事务写入（Transactional Writes）

![1615194606224](/img/1615194606224.png)

![1630426000185](/img/1630426000185.png)





​		在事务写入的具体实现上，Flink目前提供了两种方式：

> 1、预写日志（Write-Ahead-Log）`WAL`

- 把结果数据先当成状态保存，然后在收到 checkpoint 完成的通知时，一次性写入 sink 系统；
- 简单易于实现，由于数据提前在状态后端中做了缓存，所以无论什么 sink 系统，都能用这种方式；
- DataStream API 提供了一个模板类：`GenericWriteAheadSink`，来实现这种事务性 sink；



> 2、两阶段提交（`Two-Phase-Commit，2PC`）

- 对于每个 checkpoint，sink 任务会启动一个事务，并将接下来所有接收的数据添加到事务里；
- 然后将这些数据写入外部 sink 系统，但不提交它们 —— 这时只是“预提交”；
- 当它收到 checkpoint 完成的通知时，它才正式提交事务，实现结果的真正写入；
- 这种方式真正实现了 exactly-once，它需要一个提供事务支持的外部 sink 系统；
- Flink 提供了 `TwoPhaseCommitSinkFunction` 接口。



> 预写日志WAL和两阶段提交2PC区别：

![1615194699372](/img/1615194699372.png)





## 24-[掌握]-End-to-End Exactly-Once之两阶段提交

> 顾名思义，**2PC将分布式事务分成了两个阶段**，两个阶段分别为==提交请求（投票）==和==提交（执行）==。

​		Flink提供了基于2PC的SinkFunction，名为`TwoPhaseCommitSinkFunction`，做了一些基础的工作。它的第一层类继承关系如下：

![1630449987953](/img/1630449987953.png)

​		`TwoPhaseCommitSinkFunction`仍然留了以下四个抽象方法待子类来实现：

```scala
1、开始一个事务，返回事务信息的句柄。
	protected abstract TXN beginTransaction() throws Exception;

2、预提交（即提交请求）阶段的逻辑。
	protected abstract void preCommit(TXN transaction) throws Exception;
  

3、正式提交阶段的逻辑。
	protected abstract void commit(TXN transaction);

4、取消事务。
	protected abstract void abort(TXN transaction);
```



​				以Flink与Kafka的集成来说明2PC的具体流程，注意Kafka版本必须是0.11及以上，因为只有0.11+的版本才**支持幂等producer以及事务性**，从而2PC才有存在的意义。

> - 1、JobManager 协调各个 TaskManager 进行 checkpoint 存储。checkpoint保存在 StateBackend中，默认StateBackend是**内存级**的，也可以改为文件级的进行持久化保存。

![1630450463285](img/1630450463285.png)



> - 2、当开启了checkpoint ，JobManager 会将检查点分界线（barrier）注入数据流 ，barrier会在算子间传递下去；

![1630450551686](img/1630450551686.png)





> - 3、每个算子会对当前的状态做个快照，保存到状态后端，checkpoint 机制可以保证内部的状态一致性。

![1630450971971](img/1630450971971.png)



> - 4、每个内部的 transform 任务遇到 barrier 时，都会把状态存到 checkpoint 里；sink 任务首先把数据写入外部 kafka，这些数据都属于预提交的事务；遇到 barrier 时，把状态保存到状态后端，并开启新的预提交事务。

![1630451053662](img/1630451053662.png)



> - 5、当所有算子任务的快照完成，也就是这次的 checkpoint 完成时，JobManager 会向所有任务发通知，确认这次 checkpoint 完成；sink 任务收到确认通知，正式提交之前的事务，kafka 中未确认数据改为“已确认”。

![1630451107017](img/1630451107017.png)





> - 6、只有在所有检查点都成功完成这个前提下，写入才会成功。其中JobManager为协调者，各个算子为参与者（不过只有sink一个参与者会执行提交）。一旦有检查点失败，notifyCheckpointComplete()方法就不会执行。如果重试也不成功的话，最终会调用abort()方法回滚事务。

![1630451214863](img/1630451214863.png)



> Exactly-once 两阶段提交步骤总结：

- 第一条数据来了之后，开启一个 kafka 的事务（transaction），正常写入 kafka 分区日志但标记为未提交，这就是“预提交”
- jobmanager 触发 checkpoint 操作，barrier 从 source 开始向下传递，遇到 barrier 的算子将状态存入状态后端，并通知 jobmanager
- sink 连接器收到 barrier，保存当前状态，存入 checkpoint，通知 jobmanager，并开启下一阶段的事务，用于提交下个检查点的数据
- jobmanager 收到所有任务的通知，发出确认信息，表示 checkpoint 完成
- sink 任务收到 jobmanager 的确认信息，正式提交这段时间的数据
- 外部kafka关闭事务，提交的数据可以正常消费







## 25-[理解]-End-to-End Exactly-Once之Flink+Kafka一致性 

> 使用flink+kafka来实现一个端对端一致性保证：source -> transform -> sink

- **数据源Source**：Kafka Consumer 作为 Source，可以将偏移量保存下来，如果后续任务出现了故障，恢复的时候可以由连接器重置偏移量，重新消费数据，保证一致性；
- **数据转换（内部）**： 利用Checkpoin 机制，把状态存盘，发生故障的时候可以恢复，保证内部的状态一致性；
- **数据终端sink**：Kafka Producer 作为Sink，采用两阶段提交Sink，需要实现一个 TwoPhaseCommitSinkFunction

https://flink.apache.org/features/2018/03/01/end-to-end-exactly-once-apache-flink.html

![1615194848681](/img/1615194848681.png)



​			[Flink 1.4版本之后，通过两阶段提交(`TwoPhaseCommitSinkFunction`)支持End-To-EndExactly Once，而且要求Kafka 0.11+。]()

![1615194894316](/img/1615194894316.png)



​			利用`TwoPhaseCommitSinkFunction`是通用的管理方案，只要实现对应的接口，而且Sink的存储支持事务提交，即可实现端到端的精确性语义。

![1615194949827](/img/1615194949827.png)







## 26-[了解]-End-to-End Exactly-Once之Flink+MySQL一致性

​		Flink 1.11提供`JdbcSink`，查看源码实现类【`GenericJdbcSinkFunction`】可知，并没有基于【事务性】实现精确性一次语义（仅仅实现接口`CheckpointedFunction`），而是实现至少一次性语义。

https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/connectors/jdbc.html

![1615195479917](/img/1615195479917.png)

​	JDBCConnector官方文档中，明确说明，要想实现精确一次性语义，要求写入支持幂等性和Upsert语句。

![1630427609512](/img/1630427609512.png)

```
针对MySQL数据库来说：replace into + unique key
```



> 向MySQL数据库表中写入数据，可以通过实现2PC接口，实现精确性一次语义。

![1615195515712](/img/1615195515712.png)





## 27-[理解]-Flink Aysnc IO之原理及API 

> ​		Async I/O是阿里巴巴贡献给社区的一个呼声非常高的特性，于1.2版本引入。主要目的==是为了解决与外部系统交互时网络延迟成为了系统瓶颈的问题。==

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/asyncio.html

![1626855883844](/img/1626855883844.png)



> ​		流计算系统中经常需要与外部系统进行交互，通常的做法，如向数据库发送用户A的查询请求，然后等待结果返回，在之前，程序无法发送用户B的查询请求。这是一种同步访问方式，如下图所示

![1615196031516](/img/1615196031516.png)

- 1）、左图所示：通常实现方式是向数据库发送用户a的查询请求（例如在MapFunction中），然后
  等待结果返回，在这之前，无法发送用户b的查询请求，这是一种同步访问的模式，图中棕色
  的长条标识等待时间，可以发现网络等待时间极大的阻碍了吞吐和延迟；



- 2）、右图所示：为了解决同步访问的问题，`异步模式可以并发的处理多个请求和回复，`可以连续
  的向数据库发送用户a、b、c、d等的请求，与此同时，哪个请求的回复先返回了就处理哪个回复，从而连续的请求之间不需要阻塞等待，这也正是Async I/O的实现原理。



> 使用 Aysnc I/O 前提条件：

![1615196128253](img/1615196128253.png)



> Async I/O API：允许用户在数据流中使用异步客户端访问外部存储，该API处理与数据流的集成，以及消息顺序性（Order），事件时间（EventTime），一致性（容错）等脏活累活，用户只专注于业务。

- step1、使用`AysncDataStream`对数据流DataStream进行异步处理


![1615196276088](img/1615196276088.png)



- step2、自定义类，转换异步处理数据，其中需要异步请求外部存储系统，处理结果

![1615196357549](img/1615196357549.png)





## 28-[理解]-Flink Aysnc IO之异步 MySQL

> 案例说明：自定义数据源创建DataStream，依据其中字段值，采用异步方式，到MySQL数据库查询数据。
>

![1615195918515](/img/1615195918515.png)



```SQL

-- 创建数据库
CREATE DATABASE IF NOT EXISTS db_flink;

-- 使用数据库
USE db_flink ;

-- 创建表
CREATE TABLE IF NOT EXISTS db_flink.tbl_user_info (
    user_id varchar(100) NOT NULL,
    user_name varchar(255) NOT NULL,
    CONSTRAINT tbl_user_info_PK PRIMARY KEY (user_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

-- 插入数据
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1000', 'zhenshi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1001', 'zhangsan') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1002', 'lisi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1003', 'wangwu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1004', 'zhaoliu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1005', 'tianqi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1006', 'qianliu') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1007', 'sunqi') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1008', 'zhouba') ;
INSERT INTO db_flink.tbl_user_info (user_id, user_name) VALUES ('u_1009', 'wujiu') ;

```



> - 1）、第一步、编写Flink流程程序

```java
package cn.itcast.flink.asyncio;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 采用异步方式请求MySQL数据库，此处使用 Async IO实现
 */
public class _06StreamAsyncMySQLDemo {

	/**
	 * 自定义数据源，实时产生用户行为日志数据
	 */
	private static class ClickLogSource extends RichSourceFunction<String> {
		private boolean isRunning = true ;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			String[] array = new String[]{"click", "browser", "browser", "click", "browser", "browser", "search"};
			Random random = new Random();
			FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS") ;
			// 模拟用户点击日志流数据
			while (isRunning){
				String userId = "u_" + (1000 + random.nextInt(10)) ;
				String behavior = array[random.nextInt(array.length)] ;
				Long timestamp = System.currentTimeMillis();

				String output = userId + "," + behavior + "," + format.format(timestamp) ;
				System.out.println("source>>" + output);
				// 输出
				ctx.collect(output);
				// 每隔至少1秒产生1条数据
				TimeUnit.SECONDS.sleep( 1 + random.nextInt(2));
			}
		}

		@Override
		public void cancel() {
			isRunning = false ;
		}
	}

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// 2. 数据源-source
		DataStreamSource<String> dataStream = env.addSource(new ClickLogSource());

		// 3. 数据转换-transformation
		// 解析数据，获取userId，封装至二元组
		SingleOutputStreamOperator<Tuple2<String, String>> stream = dataStream.map(new MapFunction<String, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(String line) throws Exception {
				String[] array = line.split(",");
				return Tuple2.of(array[0], line);
			}
		});

		// TODO: 异步请求MySQL，采用线程池方式请求
		SingleOutputStreamOperator<String> resultDataStream = AsyncDataStream.unorderedWait(
			stream,
			new AsyncMySQLRequest(),
			1000,
			TimeUnit.MICROSECONDS,
			10
		);

		// 4. 数据终端-sink
		resultDataStream.printToErr() ;

		// 5. 触发执行-execute
		env.execute(_06StreamAsyncMySQLDemo.class.getSimpleName()) ;
	}

}
```



> - 2）、实现异步请求MySQL

```scala
package cn.itcast.flink.asyncio;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 异步请求MySQL数据库，依据userId获取userName，采用线程池方式
 */
public class AsyncMySQLRequest extends RichAsyncFunction<Tuple2<String, String>, String> {

	// 定义变量
	private Connection conn = null ;
	private PreparedStatement pstmt = null ;
	private ResultSet result = null ;

	// 定义线程池
	ExecutorService executorService = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		// 初始化线程池
		executorService = Executors.newFixedThreadPool(10);
		// a. 加载驱动类
		Class.forName("com.mysql.jdbc.Driver") ;
		// b. 获取连接
		conn = DriverManager.getConnection(
			"jdbc:mysql://node1.itcast.cn:3306/?useUnicode=true&characterEncoding=utf-8&useSSL=false",
			"root", "123456"
		);
		// c. 获取对象
		pstmt = conn.prepareStatement("SELECT user_name FROM db_flink.tbl_user_info WHERE user_id = ?");
	}

	@Override
	public void asyncInvoke(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
		// 获取用户ID
		String userId = input.f0 ;

		// 采用线程池方式，请求数据库获取用户名称
		Future<String> future = executorService.submit(new Callable<String>() {
			@Override
			public String call() throws Exception {
				// d. 设置查询值
				pstmt.setString(1, userId);
				// e. 查询数据库
				result = pstmt.executeQuery();
				// f. 获取查询的值
				if (result.next()) {
					return result.getString("user_name");
				} else {
					return "未知";
				}
			}
		});

		// 处理异步请求返回结果
		CompletableFuture.supplyAsync(new Supplier<String>() {
			@Override
			public String get() {
				try {
					return future.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					return "unknown" ;
				}
			}
		}).thenAccept(new Consumer<String>() {
			@Override
			public void accept(String result) {
				// 将原始数据与请求数据库获取数据拼凑字符串
				String output = input.f1 + "," + result ;
				// 处理结果返回给用户
				resultFuture.complete(Collections.singleton(output));
			}
		});
	}

	@Override
	public void timeout(Tuple2<String, String> input, ResultFuture<String> resultFuture) throws Exception {
		// 如果请求超时，返回数据
		String output = input.f1 + ",unknown"  ;
		// 给用户返回处理结果
		resultFuture.complete(Collections.singleton(output));
	}

	@Override
	public void close() throws Exception {
		if(null != result && ! result.isClosed()) { result.close(); }
		if(null != pstmt && ! pstmt.isClosed()) { pstmt.close(); }
		if(null != conn && ! conn.isClosed()) { conn.close(); }
	}

}
```



## [附录]-创建Maven模块

> Maven Module模块工程结构

![1603412718725](/img/1603412718725.png)



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

        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>1.18.12</version>
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
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.bahir</groupId>
            <artifactId>flink-connector-redis_${scala.binary.version}</artifactId>
            <version>1.0</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-filesystem_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-jdbc_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-csv</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-json</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!-- 指定mysql-connector的依赖 -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>${mysql.version}</version>
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

