package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.hostname("192.168.1.108")
				.port(3306)
				.databaseList("school") // set captured database
				.tableList("school.statistics_guangzhi_student") // set captured table
				.username("root")
				.password("123456")
				.serverTimeZone("UTC")
				.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
				.build();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		//读取目录下的文件
//		DataStreamSource<String> data = env.readTextFile("C:\\Program Files\\Docker\\Docker\\frontend\\resources\\app.asar.unpacked\\node_modules\\node-pty\\build\\deps\\winpty\\src\\Release\\obj\\winpty-agent\\winpty-agent.tlog\\CL.read.1.tlog");
//		//把文件中的内容按照空格进行拆分为 word,1    1 是为了能够在下面进行计算.
//		data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//					@Override
//					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//						for (String word : s.split(" ")){
//							collector.collect(new Tuple2<>(word,1));
//						}
//					}
//				}).print();


		env.enableCheckpointing(3000);


		env
			.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
			// set 4 parallel source tasks
			.setParallelism(4)
			.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
		env.execute("Print MySQL Snapshot + Binlog");

	}
}
