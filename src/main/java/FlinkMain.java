import builder.StreamEnvBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import function.JsonDebeziumDeserializationSchema;
import model.FlinkMainModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sink.AssortDorisSink;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static function.Utils.createDorisTables;

/**
 * @author wfs
 */
public class FlinkMain extends FlinkMainModel {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMain.class);

    public static void main(String[] args) throws Exception {
        ParameterTool pro =
                getProFromJar(FlinkMain.class, "/mdw-mysql2dorisSyncTool_example.properties");

        LOG.info("properties check:");
        LOG.info("##############################################################");
        for (Map.Entry<String, String> map : pro.toMap().entrySet()) {
            LOG.info("{} : {}", map.getKey(), map.getValue());
        }
        LOG.info("##############################################################");

        long checkpointInterval = pro.getLong("checkpointInterval", 60000);
        int parallelism = pro.getInt("parallelism", 2);
        // cdc setting
        String hostname = pro.getRequired("hostname");
        int port = pro.getInt("port", 3306);
        String username = pro.getRequired("username");
        String password = pro.getRequired("password");
        String databaseList = pro.getRequired("databaseList");
        String tableList = pro.getRequired("tableList");
        String serverId = pro.getRequired("serverId");

        // 自动建表
        Map<String, String> tableColumnsMap = createDorisTables(pro);

        StreamExecutionEnvironment env =
                StreamEnvBuilder.builder()
                        .setCheckpointInterval(checkpointInterval)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .setCheckpointTimeout(60000L)
                        .setMinPauseBetweenCheckpoints(5000)
                        .setTolerableCheckpointFailureNumber(3)
                        .setMaxConcurrentCheckpoints(1)
                        .setDefaultRestartStrategy(
                                5, Time.of(5, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES))
                        .setParallelism(parallelism)
                        .build();

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("time.precision.mode", "connect");
        MySqlSource<Tuple2<String, String>> source =
                MySqlSource.<Tuple2<String, String>>builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new JsonDebeziumDeserializationSchema(pro))
                        .serverId(serverId)
                        .debeziumProperties(debeziumProperties)
                        .includeSchemaChanges(true)
                        .serverTimeZone("Asia/Shanghai")
                        .startupOptions(StartupOptions.initial())
                        .build();

        SinkFunction<Tuple2<String, String>> dorisSink =
                AssortDorisSink.Builder.build(tableColumnsMap, pro);

        TypeInformation<Tuple2<String, String>> of =
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});

        // 此处sink的并行度写死为1
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .returns(of)
                .disableChaining()
                .addSink(dorisSink)
                .setParallelism(1);

        env.execute("SyncTool:" + pro.get("jobName", LocalDateTime.now().toString()));
    }
}
