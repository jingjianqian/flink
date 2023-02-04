package top.jingjianqian;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class oracleCDC {
    public static void main(String[] args) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.1.108")
                .port(1521)
                .databaseList("school") // set captured database
                .tableList("school.statistics_guangzhi_student") // set captured table
                .username("root")
                .password("123456")
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
