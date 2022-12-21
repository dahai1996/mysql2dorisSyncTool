package sink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static function.JsonDebeziumDeserializationSchema.KEY_COLUMNS;
import static function.JsonDebeziumDeserializationSchema.KEY_NEW_SINK;
import static sink.AssortDorisSink.Builder.getBuilder;
import static sink.AssortDorisSink.Builder.getProperties;

/**
 * @author wfs
 *     <p>此处主要是一个sink构造多个outputFormat实例,一个outputformat实例又对应一个streamLoad实例.
 *     即此处,持有多个streamLoad实例,组成了一个map<name,streamLoad实例>的结构.我们后续根据name来对数据分流到对应的streamLoad中
 *     <p>2022年10月19日: 此版本中,我们加入了对ddl的同步操作,加入了记录表的streamLoad实例
 *     <p><b>由此引发一个问题:因为ddl同步中涉及对streamLoad的实例进行替换操作,但是,该信息是通过数据来传递的,而一条信息只会被一个sink实例接收到.
 *     即,最后只会有一个sink实例拿到ddl变更信息,然后进行streamLoad实例的替换,而其他的sink实例拿不到变更信息,
 *     就不会对自己持有的streamLoad实例进行替换.</b>
 *     <p><b>所以,此版本sink的并行度只能为1.<b/>
 */
public class AssortDorisSink extends RichSinkFunction<Tuple2<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(AssortDorisSink.class);
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private final Map<String, DorisDynamicOutputFormatForJson<String>> outputFormatList;
    private final ParameterTool pro;

    public AssortDorisSink(
            Map<String, DorisDynamicOutputFormatForJson<String>> outputFormatList,
            ParameterTool pro) {
        this.outputFormatList = outputFormatList;
        this.pro = pro;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        for (DorisDynamicOutputFormatForJson<String> outputFormat : outputFormatList.values()) {
            outputFormat.setRuntimeContext(ctx);
            outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        }
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        try {
            outputFormatList.get(value.f0).writeRecord(value.f1);
        } catch (NullPointerException e) {
            JSONObject rowJson = JSON.parseObject(value.f1);
            if (rowJson.containsKey(KEY_NEW_SINK)) {
                LOG.info("get a create table ddl");
                LOG.info("have no sink for new table,to create a sink");

                String sinkFe = pro.get("sinkFe");
                String sinkUsername = pro.get("sinkUsername");
                String sinkPassword = pro.get("sinkPassword");
                String column = rowJson.getString(KEY_COLUMNS);
                String dorisTable = rowJson.getString("dorisTable");
                String mysqlDb = rowJson.getString("mysqlDb");
                String mysqlTable = rowJson.getString("mysqlTable");
                LogicalType[] types = {};
                String[] field = {};

                Properties sinkPro = getProperties();
                DorisOptions.Builder option = getBuilder(sinkFe, sinkUsername, sinkPassword);
                DorisExecutionOptions.Builder execution = getBuilder();

                column = String.format("%s,%s", column, DORIS_DELETE_SIGN);
                sinkPro.setProperty("columns", column);

                DorisDynamicOutputFormatForJson<String> newFormat =
                        new DorisDynamicOutputFormatForJson<>(
                                option.setTableIdentifier(dorisTable).build(),
                                DorisReadOptions.defaults(),
                                execution.setStreamLoadProp(sinkPro).build(),
                                types,
                                field);
                outputFormatList.put(mysqlDb + "." + mysqlTable, newFormat);
                RuntimeContext ctx = getRuntimeContext();

                newFormat.setRuntimeContext(ctx);
                newFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());

                LOG.info("create new sink format successfully");
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (DorisDynamicOutputFormatForJson<String> outputFormat : outputFormatList.values()) {
            outputFormat.close();
        }
    }

    public static class Builder {
        private Builder() {
            throw new IllegalStateException("AssortDorisSink Builder class");
        }

        public static AssortDorisSink build(
                Map<String, String> tableColumnsMap, ParameterTool pro) {
            // 配置
            String sinkDatabase = pro.get("sinkDatabase");
            String sinkFe = pro.get("sinkFe");
            String sinkUsername = pro.get("sinkUsername");
            String sinkPassword = pro.get("sinkPassword");

            // 统一的配置
            Properties sinkPro = getProperties();
            DorisOptions.Builder option = getBuilder(sinkFe, sinkUsername, sinkPassword);
            DorisExecutionOptions.Builder execution = getBuilder();
            LogicalType[] types = {};
            String[] field = {};

            int size = (int) (tableColumnsMap.size() / 0.75F + 1.0F);

            HashMap<String, DorisDynamicOutputFormatForJson<String>> outputMaps =
                    new HashMap<>(size);

            for (Map.Entry<String, String> v0 : tableColumnsMap.entrySet()) {
                // doris table name
                String[] v1 = v0.getKey().split("\\.");
                String dbName = v1[0];
                String tableName = v1[1];
                String sinkTable = sinkDatabase + "." + dbName + "_" + tableName;
                sinkPro.setProperty("columns", v0.getValue());

                outputMaps.put(
                        v0.getKey(),
                        new DorisDynamicOutputFormatForJson<>(
                                option.setTableIdentifier(sinkTable).build(),
                                DorisReadOptions.defaults(),
                                execution.setStreamLoadProp(sinkPro).build(),
                                types,
                                field));
            }
            // 单独添加 ddl记录表
            String ddlRecordTable = pro.getRequired("ddlRecordTable");
            outputMaps.put(
                    ddlRecordTable,
                    new DorisDynamicOutputFormatForJson<>(
                            option.setTableIdentifier(ddlRecordTable).build(),
                            DorisReadOptions.defaults(),
                            DorisExecutionOptions.defaults(),
                            types,
                            field));

            return new AssortDorisSink(outputMaps, pro);
        }

        public static DorisExecutionOptions.Builder getBuilder() {
            return DorisExecutionOptions.builder().setEnableDelete(true).setMaxRetries(5);
        }

        public static DorisOptions.Builder getBuilder(
                String sinkFe, String sinkUsername, String sinkPassword) {
            return DorisOptions.builder()
                    .setFenodes(sinkFe)
                    .setUsername(sinkUsername)
                    .setPassword(sinkPassword);
        }

        public static Properties getProperties() {
            Properties sinkPro = new Properties();
            sinkPro.setProperty("format", "json");
            sinkPro.setProperty("merge_type", "MERGE");
            sinkPro.setProperty("strip_outer_array", "TRUE");
            sinkPro.setProperty("delete", "__DORIS_DELETE_SIGN__=1");
            return sinkPro;
        }
    }
}
