package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;


public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
				.hostname("192.168.1.108")
				.port(3306)
				.databaseList("school") // set captured database
				.tableList("school.django_migrations") // set captured table
				.username("root")
				.password("123456")
				.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
				.build();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		env.enableCheckpointing(3000);


		env
			.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
			// set 4 parallel source tasks
			.setParallelism(4)
			.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
		env.execute("Print MySQL Snapshot + Binlog");

	}
}
