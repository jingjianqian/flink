package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;


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
		Properties properties = new Properties();

		//properties.put("database.url", "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(FAILOVER=ON)(LOAD_BALANCE=OFF)(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.15.105)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.15.105)(PORT=1521)))(CONNECT_DATA=warehosetest)(SERVER=DEDICATED)))");
		properties.put("database.url","jdbc:oracle:thin:@192.168.15.105:1521:warehosetest");
		SourceFunction<String> sourceFunction = OracleSource.<String>builder()
				.hostname("192.168.15.105")
				.port(1521)
				.database("warehosetest") // monitor XE database
				.schemaList("dwd_fnc") // monitor inventory schema
				.tableList("dwd_fnc.T_DWD_FNC_TARGET_MONTH") // monitor products table
				.username("dwd_fnc")
				.password("dwd_fnc")
				.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
				.debeziumProperties(properties)
				.build();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env
				.addSource(sourceFunction)
				//.addSink()
				.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
		env.execute();

//		env.setParallelism(1);
//		env.disableOperatorChaining();
//
//		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//		tableEnv.executeSql("CREATE TABLE log_test (\n" +
//					"     PROC_NAME STRING,\n" +
//					"     RUN_POINT STRING,\n" +
//					"     PRIMARY KEY(PROC_NAME) NOT ENFORCED\n" +
//					"     ) WITH (\n" +
//					"     'connector' = 'oracle-cdc',\n" +
//					"     'hostname' = '192.168.15.105',\n" +
//					"     'port' = '1521',\n" +
//					"     'username' = 'dwd_fnc',\n" +
//					"     'password' = 'dwd_fnc',\n" +
//					"     'database-name' = 'warehosetest',\n" +
//					"     'schema-name' = 'DWD_FNC',\n" +           // 注意这里要大写
//					"     'table-name' = 'T_DWD_FNC_TARGET_MONTH',\n" +
//					"     'debezium.log.mining.continuous.mine'='true',\n"+
//					"     'debezium.log.mining.strategy'='online_catalog',\n" +
//					"     'debezium.database.tablename.case.insensitive'='false',\n"+
//					"     'scan.startup.mode' = 'initial')");
//		String strSinkSql = " CREATE TABLE IF NOT EXISTS log_test_sink (" +
//							"     PROC_NAME STRING,\n" +
//							"     RUN_POINT STRING,\n" +
//							"     PRIMARY KEY(PROC_NAME) NOT ENFORCED\n" +
//							"  ) WITH (" +
//							"    'connector' = 'jdbc'," +
//							"    'url' = 'jdbc:mysql://192.168.1.108:3306/school'," +
//							"    'table-name' = 'log_test'," +
//							"    'username' = 'root'," +
//							"    'password' = '123456' " +
//							" )";
//
//		TableResult tableResult = tableEnv.executeSql("select * from log_test");
//		tableEnv.executeSql(strSinkSql);
//		tableEnv.executeSql("insert into log_test_sink select * from log_test");
//		tableResult.print();
//		env.execute();


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


//		env.enableCheckpointing(3000);
//
//
//		env
//			.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//			// set 4 parallel source tasks
//			.setParallelism(4)
//			.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
//		env.execute("Print MySQL Snapshot + Binlog");

	}
}
