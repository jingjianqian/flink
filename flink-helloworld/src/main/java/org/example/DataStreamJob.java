package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.util.Properties;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.hostname("192.168.1.108")
				.port(3306)
				.databaseList("school") // set captured database
				.tableList("school.statistics_guangzhi_student") // set captured table
				.username("flink")
				.password("flink")
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
				.schemaList("flink") // monitor inventory schema
				.tableList("flink.stu") // monitor products table
				.username("flink")
				.password("flink")
				.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
				.debeziumProperties(properties)
				.build();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env
				.addSource(sourceFunction)
				//.addSink()
				.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
		env.execute();
	}
}
