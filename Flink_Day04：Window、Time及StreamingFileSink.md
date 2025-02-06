---
stypora-copy-images-to: img
typora-root-url: ./
---



# Flink_Day04：Window、Time及StreamingFileSink



![1602831101602](/img/1602831101602.png)



```
day01：
    环境安装部署
    快速入门体验

day02：
    批处理 DataSet API使用
    Source\Transformation\Sink
    累加器、广播变量、分布式缓存

day03：
    流计算 DataStream API使用
    Source\Transformation\Sink

在Flink计算引擎中，将所有数据当做Data Stream（数据流），划分为2种：
    - 无界流unbounded data stream   
        有开始，没有结束
        流式数据
    - 有界流bounded data stream   
        有开始，没有结束
        静态数据
```

> ​		从`Flink 1.12`开始，无论批处理Batch还是流计算Stream，都是以`DataStream API`，仅设置执行模式不同即可。

![1630190237388](/img/1630190237388.png)





## 01-[复习]-上次课程内容回顾 

> Flink 分析引擎流计算`DataStream API`使用（==数据源Source、数据转换Transformation和数据终端Sink==）。

![1615078394599](/img/1615078394599.png)





## 02-[了解]-第4天：课程内容提纲

> ​		Flink 分布式计算引擎，最基础最核心：四大基石（==Window、Time==和State、Checkpoint）：

```
1）、Flink 计算引擎 四大基石概述
	概述
	
2）、Flink Window 窗口计算
	- 窗口计算概述
	- Window 类型
	- Window API
	- 不同窗口案例实战
		时间窗口TimeWindow
	
3）、Flink Time
	- Time 时间分类（语义）
	- 事件时间EventTime，数据本身字段，表示数据产生时间
	- 基于事件时间窗口分析案例（重点）
		窗口分析
		事件时间
	- 水印Watermark
		乱序数据处理
	- Allowed Lateness
		延迟数据处理，侧边流SideOutput
	
4）、并行度设置方式
	4种方式
```

> ​			讲解Flink流计算中，DataStream API中**窗口Window计算**，并且基于事件时间Eventtime窗口计算，当数据乱序`OutOfOrderness`或延迟时`Lateness`如何处理。
>





## 03-[了解]-Flink 计算引擎四大基石 

> Flink之所以能这么流行，离不开它最重要的四个基石：==Checkpoint、State、Time、Window==。

- 1）、窗口`Window`和时间`Time`
  - StructuredStreaming结构化流时，基于事件时间EventTime窗口Window计算
- 2）、状态`State`和检查点`Checkpoint`
  - Flink 流式Job运行时，有状态，可以将某个时刻的状态State进行快照和保存，称为Checkpoint

![1615079097200](/img/1615079097200.png)

- 1）、窗口Window
  - 流计算中一般在对流数据进行操作之前都会先进行开窗，即基于一个什么样的窗口上做这个计算
  - Flink提供了开箱即用的各种窗口，比如滑动窗口、滚动窗口、会话窗口以及非常灵活的自定义的窗口
  - [类似SparkStreaming中窗口计算和StructuredStreaming窗口计算]()

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html



- 2）、时间Time
  - Flink中窗口计算，基本上都是基于时间设置窗口
  - Flink还实现了Watermark的机制，能够支持基于事件时间的处理，能够容忍迟到/乱序的数据
  - [基于事件时间滚动窗口计算：EventTime事件时间、窗口计算Window、滚动窗口Tumbling]()

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_time.html



- 3）、状态State
  - Flink计算引擎，自身就是基于状态进行计算框架，默认情况下程序自己管理状态
  - 提供了一致性的语义之后，Flink为了让用户在编程时能够更轻松、更容易地去管理状态
  - 提供了一套非常简单明了的State API，包括ValueState、ListState、MapState，BroadcastState

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/



- 4）、检查点Checkpoint
  - Flink Checkpoint检查点与StructuredStreaming检查点Checkpoint类似，保存状态数据
  - 基于Chandy-Lamport算法实现了一个分布式的一致性的快照，从而提供了一致性的语义
  - 进行Checkpoint后，可以设置自动进行故障恢复
  - 此外，保存点Savepoint，人工进行Checkpoint操作，进行程序恢复执行

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/checkpointing.html





## 04-[理解]-Flink Window之窗口Window概述 

> 在Flink流式计算中，最重要以转换就是==窗口转换Window==。
>

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html

![1615046144871](/img/1615046144871.png)

​			在上述的DataStream转换图中，可以发现处处都可以对DataStream进行窗口计算。



> `窗口（window）就是从 Streaming 到 Batch 的一个桥梁`。窗口将无界流（unbounded data stream）划分很多有界流（bounded stream），对无界流进行计算：批处理。

![1615079897188](/../03_%E7%AC%94%E8%AE%B0/img/1615079897188.png)

> ​		[在实际业务需求中，往往说窗口，指定基于时间Time窗口，比如最近1分钟内数据，指的就是1分钟时间内产生的数据，放在窗口中。]()

![1615080055345](/../03_%E7%AC%94%E8%AE%B0/img/1615080055345.png)



> 窗口统计，不仅仅在流式计算中存在，批处理数据分析中也存在，比如 [开窗函数]()

```
开窗函数：
	1. 函数名称
	2. 窗口：PARTITION BY分组、ORDER BY排序、窗口大小（ROWS/RANGE)
		默认窗口大小：组内第一条数据到当前数据，组成窗口
```

![1617933668765](/img/1617933668765.png)





## 05-[理解]-Flink Window之Window 类型 

> 在Flink计算引擎中，支持窗口类型很多种，基本Streaming流式计算引擎需要实现窗口都支持。
>

![1615081289465](/img/1615081289465.png)

> - 1）、时间窗口`TimeWindow`
>   - [按照时间间隔划分出窗口，并对窗口中数据进行计算]()
>   
>   - 在SparkStreaming和StructuredStreaming中窗口计算，都是时间窗口，属于==滑动窗口==
>   
>   - 滚动（Tumbling）窗口和滑动（Sliding）窗口
>   
>    
> - 2）、计数窗口`CountWindow`
>   - 按照`数据条目数`进行设置窗口，比如每10条数据统计一次
>   - 滚动窗口和滑动窗口
>   
>   
> - 3）、会话窗口`SessionWindow`
>   - 会话Session相关，表示多久没有来数据，比如5分钟都没有来数据，将前面的数据作为一个窗口
>   - 当登录某个网站，超过一定时间对网站网页未进行任何操作（比如30分钟），自动推出登录，需要用户重新登录，Session会话超时
>



![1630193207181](/img/1630193207181.png)

```
1、滚动窗口（翻滚窗口或者固定窗口）
	基于时间滚动窗口
	基于计数滚动窗口
	
2、滑动窗口
	基于滑动窗口
	基于计数滑动窗口
	
3、会话窗口
	基于时间，设置超时时间间隔
```



## 06-[掌握]-Flink Window之时间与计数窗口 

> - 1）、==time-window（时间窗口）==
>   - 根据`时间划分`窗口，如每xx分钟统计，最近xx分钟的数据
>   - 在Spark框架中，SparkStreaming和StructuredStreaming窗口统计都是基于时间窗口
>   - [实时分析：趋势分析]()
>   



> 
> - 2）、==count-window（计数窗口）==：
>   - 根据`数量划分`窗口，如每xx个数据统计，最近xx个数据
>   - [此种方式窗口计算，在实际项目中使用不多，但是有些特殊业务需要，需要使用此场景。]()

![1615082260974](/img/1615082260974.png)





## 07-[掌握]-Flink Window之滑动与滚动窗口 

在Flink窗口计算中，无论时间窗口还是计数窗口，都可以分为2种类型：`滚动Tumbling窗口和滑动Sliding窗口`

> [窗口有两个重要的属性: 窗口`大小size`和滑动`间隔slide`]()



> - 1）、滚动窗口（Tumbling Window）
> - 条件：**窗口大小size  =   滑动间隔 slide**

![1615083122099](/img/1615083122099.png)



> - 2）、滑动窗口（Sliding Window）
>   - 条件：**窗口大小 != 滑动间隔**，通常条件【`窗口大小size > 滑动间隔slide`】

![1615083144138](/img/1615083144138.png)



> 上面图中展示的以计数窗口CountWindow讲解滚动窗口和滑动窗口。
>

![1615083194778](/img/1615083194778.png)



> 按照上面窗口的分类方式进行组合，可以得出如下的窗口：

![1615083238234](/img/1615083238234.png)





## 08-[掌握]-Flink Window之Window API

​	在Flink流计算中，提供Window窗口API分为两种：

> - 1）、针对`KeyedStream`窗口API：`window`
>
>     [先对数据流DataStream进行分组`keyBy`，再设置窗口`window`，最后进行聚合`apply`操作。]()
>
>   - 第一步、数据流DataStream调用`keyBy`函数分组，获取`KeyedStream`
>   - 第二步、`KeyedStream.window`设置窗口
>   - 第三步、聚合操作，对窗口中数据进行聚合统计
>     - 函数：reduce、fold、aggregate函数
>     - `apply()` 函数
>

![1615083436007](/img/1615083436007.png)

```scala
// TODO: KeyedStream窗口操作，先分组，再窗口，最后聚合
SingleOutputStreamOperator<String> windowDataStream = tupleStream
    // 设置Key进行分组
    .keyBy(0)
    // 设置窗口，每5秒中统计最近5秒的数据，滚动窗口
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    // 聚合操作
    .apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
        @Override
        public void apply(Tuple tuple, 
                          TimeWindow window, 
                          Iterable<Tuple2<String, Integer>> input, 
                          Collector<String> out) throws Exception {

        }
    });
```

![1626749818052](/img/1626749818052.png)



> - 2）、非KeyedStream窗口API：`windowAll`
>   - 直接调用窗口函数：`windowAll`，然后再对窗口所有数据进行处理，未进行分组
>   - 聚合操作，对窗口中数据进行聚合统计
>     - 函数：`reduce、fold、aggregate`函数
>     - `apply()` 函数

![1615083564491](/img/1615083564491.png)

```JAVA
// TODO: 未进行分组，直接窗口window，再聚合
SingleOutputStreamOperator<String> allWindowDataStream = tupleStream
	.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
	.apply(new AllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
		@Override
		public void apply(TimeWindow window,
		                  Iterable<Tuple2<String, Integer>> values,
		                  Collector<String> out) throws Exception {

		}
	});
```

![1626749975676](/img/1626749975676.png)





> [在实际项目，Flink流计算，如果对DataStream流进行窗口计算时，通常使用`apply`函数]()

![1626747087568](/../03_%E7%AC%94%E8%AE%B0/img/1626747087568.png)

> ​		当对DataStream数据流划分窗口window以后，获取到数据流：`WindowedStream`，其中`apply`方法，传递`WindowFunction`函数处理实体类，**定义如何对窗口数据处理**。

![1626699472210](/../03_%E7%AC%94%E8%AE%B0/img/1626699472210.png)





## 09-[掌握]-Flink Window之案例【时间滚动窗口】

> 基于时间滚动窗口：`size大小 = slide 大小`
>
> - 滚动窗口能将数据流切分成**不重叠的窗口**，每一个事件只能属于一个窗口
> - 滚动窗口具有固定的尺寸，不重叠。

![1615085859901](/img/1615085859901.png)



> 时间窗口案例说明：==信号灯编号和通过该信号灯的车的数量==
>
> [智慧城市智能交通各个卡（qia）口实时流量监测]()

![1626750306202](/img/1626750306202.png)

```
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
```

> 通过netcat工具，发送数据：`nc -lk 9999`
>
> [需求：每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量]()

```scala
package cn.itcast.flink.window.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口统计案例演示：滚动时间窗口（Tumbling Time Window），实时交通卡口车流量统计
 */
public class StreamTumblingTimeWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据：
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().split(",").length == 2)
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
				}
			});
		
		// TODO: 先按照卡口分组，再进行窗口操作，最后聚合累加
		SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = mapDataStream
			.keyBy(0) // 下标索引，卡口编号
			.timeWindow(Time.seconds(5))  // 滚动时间窗口，仅仅设置窗口大小即可
			.sum(1);

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamTumblingTimeWindow.class.getSimpleName()) ;
	}

}
```





## 10-[掌握]-Flink Window之案例【时间滑动窗口】

> 需求：==每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量==

![1615085998488](/img/1615085998488.png)

```scala
package cn.itcast.flink.window.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口统计案例演示：滑动时间窗口（Sliding Time Window），实时交通卡口车流量统计
 */
public class StreamSlidingTimeWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据：
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().split(",").length == 2)
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
				}
			});

		// TODO: 先按照卡口分组，再进行窗口操作，最后聚合累加
		SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = mapDataStream
			.keyBy(0) // 下标索引，卡口编号
			// public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide) 
			.timeWindow(Time.seconds(10), Time.seconds(5))  // 滚动时间窗口，仅仅设置窗口大小即可
			.sum(1);

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamSlidingTimeWindow.class.getSimpleName()) ;
	}

}

```

> 针对滑动窗口来说：`size > slide`时，数据会被重复计算。
>
> [在窗口统计中，滚动窗口可以认为是滑动窗口特殊情况`：size=slide`]()





## 11-[理解]-Flink Window之案例【计数滚动窗口】

> Flink流计算支持计数窗口CountWindow，首先看案例：[每2个元素计算1次最近4个元素的总和]()
>

![1615086415357](/img/1615086415357.png)



> 需求：**统计在最近5条消息中，数字之和sum值**
>

```
1
1
1
2
3
4
5
6
4
3
```

```scala
package cn.itcast.flink.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 窗口统计案例演示：滚动计数窗口（Tumbling Count Window)，数字累加求和统计
 */
public class StreamTumblingCountWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据格式：
1
1
1
2
3
4
5
5
 */

		SingleOutputStreamOperator<Tuple1<Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().length() > 0)
			.map(new MapFunction<String, Tuple1<Integer>>() {
				@Override
				public Tuple1<Integer> map(String line) throws Exception {
					return Tuple1.of(Integer.parseInt(line));
				}
			});

		// TODO: 滚动计数窗口设置，不进行key分组，使用windowAll
		SingleOutputStreamOperator<Tuple1<Integer>> sumDataStream = mapDataStream
			.countWindowAll(5) // 设置数量大小为5
			.sum(0);

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamTumblingCountWindow.class.getSimpleName()) ;
	}

}
```





## 12-[理解]-Flink Window之案例【计数滑动窗口】

> 需求：**每隔2条数据，统计在最近5条消息中， 数字之和sum值**

```java
package cn.itcast.flink.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 窗口统计案例演示：滑动计数窗口（Tumbling Count Window)，数字累加求和统计
 */
public class StreamSlidingCountWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据格式：
1
1
1
2
3
4
5
5
 */

		SingleOutputStreamOperator<Tuple1<Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().length() > 0)
			.map(new MapFunction<String, Tuple1<Integer>>() {
				@Override
				public Tuple1<Integer> map(String line) throws Exception {
					return Tuple1.of(Integer.parseInt(line));
				}
			});

		// TODO: 滚动计数窗口设置，不进行key分组，使用windowAll
		SingleOutputStreamOperator<Tuple1<Integer>> sumDataStream = mapDataStream
			.countWindowAll(5, 2) // 设置数量大小为5
			.sum(0);

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamSlidingCountWindow.class.getSimpleName()) ;
	}

}

```

>  			​			计数滑动窗口与时间滑动窗口，数据都会有一部分被重复计算，一个是依据时间划分窗口，一个是依据数据数量划分窗口。





## 13-[了解]-Flink Window之案例【时间会话窗口】

> Flink流计算中支持：会话窗口Session，但是基于`时间`的，需要设置`超时时间间隔grap`
>
> - 会话窗口不重叠，没有固定的开始和结束时间
> - 与翻滚窗口和滑动窗口相反, ==当会话窗口在一段时间内没有接收到元素时会关闭会话窗口==。
> - 后续的元素将会被分配给新的会话窗口
>

需求：`设置会话超时时间为10s，10s内没有数据到来，则触发上个窗口的计算`

![1615087428867](/img/1615087428867.png)

```java
package cn.itcast.flink.window.session;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口统计案例演示：时间会话窗口（Time Session Window)，数字累加求和统计
 */
public class StreamSessionWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据格式：
1
1
1
2
3
4
5
5
 */

		SingleOutputStreamOperator<Tuple1<Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().length() > 0)
			.map(new MapFunction<String, Tuple1<Integer>>() {
				@Override
				public Tuple1<Integer> map(String line) throws Exception {
					return Tuple1.of(Integer.parseInt(line));
				}
			});

		// TODO: 滚动计数窗口设置，不进行key分组，使用windowAll
		SingleOutputStreamOperator<Tuple1<Integer>> sumDataStream = mapDataStream
			.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
			.sum(0) ;

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamSessionWindow.class.getSimpleName()) ;
	}

}
```





## 14-[掌握]-Flink Window之window及apply方法

> ​			在前面讲解Window窗口案例中，尤其是**时间窗口TimeWindow**中，没有看见`Window`大小（起==始时间startDateTime，结束时间endDateTime==），使用`apply`函数，就可以获取窗口大小。

![1630292179840](/img/1630292179840.png)



> 具体演示代码如下：

```scala
package cn.itcast.flink.window.apply;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口统计案例演示：滑动时间窗口（Sliding Time Window），实时交通卡口车流量统计
 */
public class StreamSlidingTimeWindowApply {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 2. 数据源-source
		DataStreamSource<String> inputDataStream = env.socketTextStream("node1.itcast.cn", 9999);

		// 3. 数据转换-transformation
/*
数据：
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */
		SingleOutputStreamOperator<Tuple2<String, Integer>> mapDataStream = inputDataStream
			.filter(line -> null != line && line.trim().split(",").length == 2)
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
				}
			});

		// TODO: 先按照卡口分组，再进行窗口操作，最后聚合累加
		SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = mapDataStream
			.keyBy(0) // 下标索引，卡口编号
			// public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide)
			.timeWindow(Time.seconds(10), Time.seconds(5))  // 滚动时间窗口，仅仅设置窗口大小即可
			.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") ;

				@Override
				public void apply(Tuple tuple, // 表示分组Key，封装到元组中
				                  TimeWindow window, // 时间窗口，获取startTime和endTime
				                  Iterable<Tuple2<String, Integer>> input, // 窗口中数据，进行聚合操作
				                  Collector<Tuple2<String, Integer>> out) throws Exception {
					// 获取分组Key
					String  key = (String)((Tuple1)tuple).f0 ;

					// 获取窗口开始时间和结束时间
					long start = window.getStart();
					long end = window.getEnd();

					// 输出内容
					String output = "[" + format.format(start) + "~" + format.format(end) + "] -> " + key ;

					// 对窗口中的数据进行聚合
					int count = 0 ;
					for (Tuple2<String, Integer> item: input){
						count += item.f1 ;
					}

					// 最后输出
					out.collect(Tuple2.of(output, count));
				}
			});

		// 4. 数据终端-sink
		sumDataStream.printToErr();

		// 5. 触发执行-execute
		env.execute(StreamSlidingTimeWindowApply.class.getSimpleName()) ;
	}

}
```

> 运行程序，进行测试：

![1615089665321](/img/1615089665321.png)







## 15-[掌握]-Flink Time之Time 时间语义

> 在Flink的流式处理中，会涉及到时间的不同概念，如下图所示：

![1626700926004](/img/1626700926004.png)

> - 1）、事件时间`EventTime`
>   - 事件真真正正发生产生的时间，比如订单数据中订单时间表示订单产生的时间
> - 2）、摄入时间`IngestionTime`
>   - 数据被流式程序获取的时间
> - 3）、处理时间`ProcessingTime`
>   - 事件真正被处理/计算的时间

![1615090259364](/img/1615090259364.png)



```
在Flink流计算中，关于时间语义支持三种，其中2种语义最为关键：
1、处理时间`ProcessingTime`，使用处理时间
   SparkStreaming时，窗口计算，使用的就是处理时间

2、事件时间`EventTime`，最为合理
   StructuredStreaming中，窗口计算，使用的是事件时间

```



> 流式数据处理中，处理时间ProcessingTime和事件时间EventTime区别如下：

![1615090899665](/img/1615090899665.png)







## 16-[复习]-Flink Window窗口类型

> [窗口Window计算，属于Streaming流计算到Batch批处理一个桥梁。]()

```ini
1、Flink Window窗口类型划分：
	第一种方式：
		时间窗口TimeWindow，以时间范围划分流式数据为窗口
		计数窗口CountWindow，以数据量为标准划分流式数据为窗口
		会话窗口SessionWindow
		
	第二种方式：
		滚动窗口ThumblingWindow，窗口大小size=滑动大小slide
		滑动窗口SlidingWindow
		会话窗口SessionWindow
		
2、Flink Time 时间语义
	主要针对Flink Window，时间窗口进行划分的
	语义三种：
		事件时间，数据产生的时间，包含在数据中某个字段
		摄取时间，流式计算框架获取数据的时间，比如Source操作
		处理时间，数据被计算处理的时间，比如Transformation操作
	
	如果按照上述时间语义，结合时间窗口，可以有如下几种窗口：
		基于事件时间EventTime滚动窗口     ★ ★ ★ ★ ★
		基于事件时间EventTime滑动窗口		★ ★ ★ ★ ★
		
		基于处理时间ProcessingTime滚动窗口		★ ★ ★ ★ 
		基于处理时间ProcessingTime滑动窗口		★ ★ ★ ★ 

【
		基于摄取时间IngestionTime滚动窗口	
		基于摄取时间IngestionTime滑动窗口		
】		
		
	会话窗口SessionWindow，基于时间设置的窗口
		基于事件时间会话窗口						★ ★ ★ ★ ★
		基于处理时间会话窗口						★ ★ ★ ★ 
```

![1629269018761](/../03_%E7%AC%94%E8%AE%B0/img/1629269018761.png)



> 在Flink Stream流式应用程序中，使用StreamExecutionEnvironment设置时间语义：

![1630293880982](/img/1630293880982.png)

> 时间语义，使用枚举定义：

![1630293913167](/img/1630293913167.png)





## 17-[理解]-Flink Time之EventTime重要性 

​		通过几个示例，感受对数据处理时，需要使用基于**事件时间EventTime**进行分析，更加合理化。

> - 1）、==示例一：外卖订单处理==

![1615090479867](/img/1615090479867.png)



> - 2）、==示例二：错误日志时段统计==

![1615090587858](/img/1615090587858.png)



> 3）、==示例三：用户抢单网络延迟==

![1615090747244](/img/1615090747244.png)



> - 4）、`示例四：网站最近实时PV统计`

![1615090834130](/img/1615090834130.png)





## 18-[掌握]-Flink Time之事件时间案例【编程】

[基于事件时间EventTime窗口分析时，要求数据字段中，必须包含事件时间字段，代表数据产生时间。]()

> 需求：==基于事件时间EventTime Tumbling Window窗口【5秒】，进行聚合统计：WordCount。==

```
1000,a,1
2000,a,1
5000,a,1
9999,a,1
11000,a,2
14000,b,1
14999,b,1
```



​			使用基于事件时间`EventTime`窗口统计，需要如下三个步骤：

> - **第一步、设置时间语义为：EventTime 语义**
>    - 默认情况下，使用时间语义为：处理时间ProcessingTime

![1630238911673](/img/1630238911673.png)



> - **第二步、设置事件时间字段，数据类型必须为`Long`类型**

![1630238983897](/img/1630238983897.png)



> - **第三步、设置基于事件时间EventTime滚动窗口大小：size**

![1630239112273](/img/1630239112273.png)



​		完整案例代码如下：

```scala
package cn.itcast.flink.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口统计案例演示：滚动事件时间窗口（Tumbling EventTime Window），窗口内数据进行词频统计
 */
public class _09StreamEventTimeWindow {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// TODO: step1. 设置时间语义为事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 2. 数据源-source
		DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

		// TODO: step2. 设置事件时间字段，数据类型必须为Long类型
		SingleOutputStreamOperator<String> timeStream = inputStream
			.filter(line -> null != line && line.trim().split(",").length == 3)
			.assignTimestampsAndWatermarks(
				// 此时，不允许数据延迟，如果延迟，就不处理数据
				new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
					@Override
					public long extractTimestamp(String line) {
						return Long.parseLong(line.trim().split(",")[0]);
					}
				}
			);

		// 3. 数据转换-transformation
/*
1000,a,1
2000,a,1
5000,a,1
9999,a,1
11000,a,2
14000,b,1
14999,b,1
 */
		SingleOutputStreamOperator<String> windowStream = timeStream
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					// 打印每条数据
					System.out.println(format.format(Long.parseLong(split[0])) + "," + split[1] + "," + split[2]);
					return Tuple2.of(split[1], Integer.parseInt(split[2]));
				}
			})
			// 先分组, 元组数据类型是，使用下标索引
			.keyBy(0)
			// TODO: step3. 设置窗口为5秒
			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
			// 窗口内数据聚合
			.apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
				@Override
				public void apply(Tuple tuple,
				                  TimeWindow window,
				                  Iterable<Tuple2<String, Integer>> input,
				                  Collector<String> out) throws Exception {
					// a. 获取分组Key
					String  key = (String)((Tuple1)tuple).f0 ;
					// b. 获取窗口开始时间和结束时间
					String startDate = format.format(window.getStart());
					String endDate = format.format(window.getEnd());
					// c. 对窗口中的数据进行聚合
					int total = 0 ;
					for (Tuple2<String, Integer> item: input){
						total += item.f1 ;
					}
					// d. 输出内容
					String output = "window[" + startDate + "~" + endDate + "] -> " + key + " = " + total ;
					// e. 最后输出
					out.collect(output);
				}
			});

		// 4. 数据终端-sink
		windowStream.printToErr();

		// 5. 触发执行-execute
		env.execute(_09StreamEventTimeWindow.class.getSimpleName()) ;
	}

}
```

> ​		在实际项目开发中，往往对流式数据进行窗口统计时，需要指定窗口是什么（**窗口开始时间和窗口结束时间**），此时需要使用`apply`函数，进行窗口数据聚合操作，其中可以获取窗口信息。
>





## 19-[掌握]-Flink Time之事件时间案例【测试】

> 基于时间窗口TimeWindow来说，每个窗口都有【开始时间start】和【结束时间end】，属于左闭右开。

![1630295387612](/img/1630295387612.png)



```ini
窗口：
	2021-03-07 14:00:00    -    2021-03-07 14:10:00
	左闭（包含）右开（不包含）
	[14:00:00, 14:10:00) 
				windowEnd ： 14:09:59.999   少1ms
	[14:00:00, 14:09:59.999]
	
数据：
	2021-03-07 14:00:00,user1001,11
	2021-03-07 14:04:00,user1002,22
	2021-03-07 14:05:00,user1001,55
	2021-03-07 14:09:59.999,user1001,66 a
	
	2021-03-07 14:10:00,user1003,23
```



[当基于事件时间Time窗口分析时，窗口数据什么进行触发计算呢？？？？？]()

`默认情况下（不考虑乱序和延迟），当数据事件时间EventTime >= 窗口结束时间，触发窗口数据计算。`



> 运行Flink流式计算程序，在CRT命令，通过`nc -lk 9999` 输入如下数据：

```
1000,a,1
2000,a,1
5000,a,1

9999,a,1

11000,a,2
14000,b,1
14999,b,1

```

> 运行结构显示如下所示：

![1615101230968](/img/1615101230968.png)





## 20-[掌握]-Flink Time之事件时间案例【原理】

> ​		基于事件时间EventTime窗口分析，如果不考虑数据延迟乱序，当窗口被触发计算以后，延迟乱序到达的数据将不会被计算，而是直接丢弃。

![1630240597815](/img/1630240597815.png)





## 21-[掌握]-Flink Time之EventTime窗口起始时间

> 基于事件时间EventTime窗口分析时，==第一个窗口的起始时间==是如何确定的呢？？

```
第一条数据：1970-01-01 08:00:01,a,1
	第一个窗口起始时间：1970-01-01 08:00:00
```



> [第一个窗口起始时间，依据**第一条数据的事件时间**计算得到的。]()

- 首先依据第一条数据的事件时间计算第一个窗口开始时间；
- 再依据窗口大小，计算出第一个窗口时间范围；
- 最后，根据窗口大小与滑动大小进行计算第二个窗口，第三个窗口，以此类推下一个窗口时间范围；



![1615101646594](/img/1615101646594.png)

> 假设第一条数据：`1000,a,3`，那么计算第一个窗口起始时间:`1970-01-01 08:00:00`
>

![1615101888593](/img/1615101888593.png)





## 22-[理解]-Flink Time之引入 Watermaker

> ​		基于事件时间EventTime窗口分析，默认情况下，如果某个窗口触发计算以后，再来一条窗口内的数据，此时不会计算这条数据，而是直接丢弃。

![1615103309161](/img/1615103309161.png)



> ​			在实际业务数据中，数据乱序到达流处理程序，属于正常现象，原因在于**网络延迟导致数据延迟，无法避免的**，所以应该可以[允许数据延迟达到（在某个时间范围内），依然参与窗口计算]()。

​			比如允许数据最大乱序延迟时间为2秒，那么此时只要符合时间范围乱序数据都会处理，此种机制：**Watermark水印机制**。

> [水印机制Watermark：允许数据乱序到达，在对应窗口中进行计算（延迟时间很短）]()



案例演示：修改上面代码，**设置最大允许乱序时间为2秒，运行程序测试**。

![1615103748650](/img/1615103748650.png)

> 运行结果分析

![1615103699683](/img/1615103699683.png)





## 23-[理解]-Flink Time之Watermaker 是什么 

> - 1）、Watermark水位线定义：

![1615103873951](/img/1615103873951.png)





> - 2）、Watermaker 如何计算：

![1615103936739](/img/1615103936739.png)



[针对窗口计算时，Watermar值计算，参考整个窗口中数据计算]()

![1615103993941](/img/1615103993941.png)





> - 3）、Watermaker 有什么用
>

[Watermaker是用来触发窗口计算的！]()



> - 4）、Watermaker 如何触发窗口计算

![1615104147743](/img/1615104147743.png)

```
Watermaker = 当前窗口的最大的事件时间 - 最大允许的延迟时间或乱序时间  & Watermaker >= 窗口的结束时间
				|
当前窗口的最大的事件时间 - 最大允许的延迟时间或乱序时间 >= 窗口的结束时间
				|
当前窗口的最大的事件时间 >= 窗口的结束时间 + 最大允许的延迟时间或乱序时间
```



> 当设置水位WaterMark以后，窗口触发计算和乱序数据达到处理流程

![1630241199635](/img/1630241199635.png)



> ​		基于事件时间窗口统计，往往存在数据乱序达到（由于网络延迟原因），所以设置watermark水印机制，允许数据短时间内延迟达到，依然进行处理数据。





## 24-[理解]-Flink Time之Allowed Lateness 功能 

​		[默认情况下，当watermark通过end-of-window之后，再有之前的数据到达时，这些数据会被删除。]()为了避免有些迟到的数据被删除，因此产生了**allowedLateness**的概念。

- 简单来讲，allowedLateness就是针对event time而言，对于watermark超过end-of-window之后，还[允许有一段时间（也是以event time来衡量）来等待之前的数据到达，以便再次处理这些数据]()。
- 默认情况下，如果不指定allowedLateness，其值是0，即对于watermark超过end-of-window之后，还有此window的数据到达时，这些数据被删除掉了。



> `延迟数据`是指：在当前窗口【假设窗口范围为10-15】已经计算之后，又来了一个属于该窗口的数据【假设事件时间为13】，这时候仍`会触发window操作`，这种数据就称为延迟数据。

​			Allowed Lateness 机制允许用户设置一个允许的最大迟到时⻓。Flink 会在窗口关闭后一直保存窗口的状态直至超过允许迟到时⻓，这期间的迟到事件不会被丢弃，而是默认会触发窗口重新计算**。因为保存窗口状态需要额外内存**，并且如果窗口计算使用了ProcessWindowFunction， API 还可能使得每个迟到事件触发一次窗口的全量计算，**代价比较大，所以允许迟到时⻓不宜设得太⻓，迟到事件也不宜过多**，否则应该考虑降低水位线提高的速度或者调整算法。



> 在window窗口方法后，再次调用【`allowedLateness`】方法，设置AllowedLateness延迟数据时长。

![1630247637804](/img/1630247637804.png)

> WindowStream流中`allowedLateness`方法源码截图：

![1615107668260](/img/1615107668260.png)



> ​		此外，如果延迟数据超过设置allowedLateness时长到达，可以通过侧边流Side OutputTag保存延迟数据，然后进行单独处理，代码如下所示：

![1630247813281](/img/1630247813281.png)





> [针对基于事件时间EventTime窗口分析，如何解决乱序数据和延迟数据的呢？？？]()

- **1）、乱序数据：Watermark**
  - 使用水位线Watermark，给每条数据加上一个时间戳
  - Watermark = 数据事件时间 - 最大允许乱序时间
  - 当数据的Watermark >= 窗口结束时间，并且窗口内有数据，触发窗口数据计算
- **2）、延迟数据：AllowedLateness**
  - 设置方法参数：`allowedLateness`，表示允许延迟数据最多可以迟到多久，还可以进行计算（保存窗口，并且触发窗口计算）
  - [当某个窗口触发计算以后，继续等待多长时间，如果在等待时间范围内，有数据达到时，依然会触发窗口计算。如果到达等待时长以后，没有数据达到，销毁窗口数据信息。]()



> 用`AllowedLateness` 与 `SideOutputLateData`处理迟到数据

- **AllowedLateness**: 窗口销毁前，迟到数据，会继续触发窗口计算。
  - Globle Window, 此值默认Long.MAX_VALUE,即窗口永远不会结束；
  - 其他窗口，此值默认0，即直接丢弃，不再触发窗口计算。
- **SideOutputLateData**: 窗口销毁后，依然迟到的数据，可单独收集起来。
  - 默认无。
  - 如当AllowedLateness设置为0，即可通过SideOutputLateData将所有迟到数据都收集起来，另做处理。
    



## 25-[掌握]-Flink Time之Allowed Lateness 案例 

> ​		修改上述基于事件时间窗口统计分析代码，加上允许延迟数据：`5秒`，并且将延迟数据放到侧边流中，进行额外单独处理分析。

```java
package cn.itcast.flink.eventtime;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 窗口统计案例演示：滚动事件时间窗口（Tumbling EventTime Window），窗口内数据进行词频统计
 */
public class _10StreamEventTimeWindowAllowedLateness {

	public static void main(String[] args) throws Exception {
		// 1. 执行环境-env
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1) ;

		// TODO: step1. 设置时间语义为事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// 2. 数据源-source
		DataStreamSource<String> inputStream = env.socketTextStream("node1.itcast.cn", 9999);

		// TODO: step2. 设置事件时间字段，数据类型必须为Long类型
		SingleOutputStreamOperator<String> timeStream = inputStream
			.filter(line -> null != line && line.trim().split(",").length == 3)
			.assignTimestampsAndWatermarks(
				// TODO：设置允许最大乱序时间为2秒
				new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
					@Override
					public long extractTimestamp(String line) {
						return Long.parseLong(line.trim().split(",")[0]);
					}
				}
			);

		// 3. 数据转换-transformation
/*
1000,a,1
2000,a,1
5000,a,1
9999,a,1
11000,a,2
14000,b,1
14999,b,1
 */
		// 定义延迟数据标签Tag
		final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-data"){};

		SingleOutputStreamOperator<String> windowStream = timeStream
			.map(new MapFunction<String, Tuple2<String, Integer>>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

				@Override
				public Tuple2<String, Integer> map(String line) throws Exception {
					String[] split = line.trim().split(",");
					// 打印每条数据
					System.out.println(format.format(Long.parseLong(split[0])) + "," + split[1] + "," + split[2]);
					return Tuple2.of(split[1], Integer.parseInt(split[2]));
				}
			})
			// 先分组, 元组数据类型是，使用下标索引
			.keyBy(0)
			// TODO: step3. 设置窗口为5秒
			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
			// TODO: 设置允许的延迟数据的时间，某个窗口数据被触发计算以后，在等待时间，如果该窗口有数据达到，依然可以触发计算
			.allowedLateness(Time.seconds(5))
			// TODO: 如果数据到达超过设置允许延迟时间值，直接将其放到侧边流中，单独进行获取与处理分析
			.sideOutputLateData(lateOutputTag)
			// 窗口内数据聚合
			.apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
				private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
				@Override
				public void apply(Tuple tuple,
				                  TimeWindow window,
				                  Iterable<Tuple2<String, Integer>> input,
				                  Collector<String> out) throws Exception {
					// a. 获取分组Key
					String  key = (String)((Tuple1)tuple).f0 ;
					// b. 获取窗口开始时间和结束时间
					String startDate = format.format(window.getStart());
					String endDate = format.format(window.getEnd());
					// c. 对窗口中的数据进行聚合
					int total = 0 ;
					for (Tuple2<String, Integer> item: input){
						total += item.f1 ;
					}
					// d. 输出内容
					String output = "window[" + startDate + "~" + endDate + "] -> " + key + " = " + total ;
					// e. 最后输出
					out.collect(output);
				}
			});

		// 4. 数据终端-sink
		windowStream.printToErr();

		// TODO: 获取侧边流
		windowStream.getSideOutput(lateOutputTag).printToErr("late>>>");

		// 5. 触发执行-execute
		env.execute(_08StreamEventTimeWindow.class.getSimpleName()) ;
	}

}
```



> ​		基于EventTime处理数据，会有乱序或迟到的问题，乱序借助`Watermark`来解决，`MaxOutOfOrderness`或`AllowedLateness`都是用来解决迟到问题的。
>
> - `MaxOutOfOrderness`: 第一次窗口计算触发前，最多允许乱序或迟到多久。
> - `AllowedLateness`: 窗口计算触发后，依然有迟到，最多还允许迟到多久。

- `AllowedLateness`: 窗口计算触发后，依然有迟到，最多还允许迟到多久。
- 一般情况下，窗口计算再次或多次触发的时机: `Watermark` < `Window End Time` + `AllowedLateness`。
- 窗口销毁的时机: `Watermark` >= `Window End Time` + `AllowedLateness`。

![1630246901662](/img/1630246901662.png)

> - 1、窗口window 的作用是**为了周期性的获取数据**
> - 2、watermark作用是**防止数据出现乱序(经常)**，事件时间内获取不到指定的全部数据，做的一种保险方法
> - 3、allowLateNess是**将窗口关闭时间再延迟一段时间**
> - 4、sideOutPut是最后兜底操作，**所有过期延迟数据，指定窗口已经彻底关闭，就会把数据放到侧输出流**





## 26-[掌握]-Flink 原理之并行度设置 

> 一个Flink程序由多个Operator组成(`source、transformation和 sink`)。

![1630241888273](/img/1630241888273.png)



> ​		一个Operator由多个并行的SubTask（以线程方式）来执行， 一个Operator的并行SubTask(数目就被称为该Operator(任务)的并行度(Parallelism)。

![1630241797904](/img/1630241797904.png)



在Flink 中，任务的并行度设置可以从4个层次级别指定，具体如下所示：

![1630243097602](/img/1630243097602.png)

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/parallel.html



- 1）、==Operator Level（算子级别）==(可以使用)

> 一个operator、source和sink的并行度可以通过调用 `setParallelism()`方法来指定。

![1630243927963](/img/1630243927963.png)





- 2）、==Execution Environment Level==（Env级别，可以使用)

> ​		执行环境(任务)的默认并行度可以通过调用`setParallelism()`方法指定。

![1630243963333](/img/1630243963333.png)





- 3）、==Client Level==(客户端级别，推荐使用)

> 并行度可以在客户端将job提交到Flink时设定，对于CLI客户端，可以通过`-p`参数指定并行度

![1630244027260](/img/1630244027260.png)





- 4）、==System Level==（系统默认级别，尽量不使用）

> ​			在系统级可以通过设置`flink-conf.yaml`文件中的`parallelism.default`属性来指定所有执行环境的默认并行度。



总结：并行度的优先级：`算子级别 > env级别 > Client级别 > 系统默认级别` 

- 1）、如果source不可以被并行执行，即使指定了并行度为多个，也不会生效

- 2）、实际生产中，推荐`在算子级别显示指定各自的并行度`，方便进行显示和精确的资源控制。
- 3）、slot是静态的概念，是**指taskmanager具有的并发执行能力**; `parallelism`是动态的概念，是指`程序运行时实际使用的并发能力`。





## [附录]-创建Maven模块

> Maven Module模块工程结构

![1603238758540](/img/1603238758540.png)

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

        <!-- 指定mysql-connector的依赖 -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>${mysql.version}</version>
        </dependency>

        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.68</version>
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

