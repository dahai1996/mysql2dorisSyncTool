package function;

import bean.ColumnInfo;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static function.Utils.quote;

/**
 * @author wfs 自定义一个解析函数，将数据解析为 <库表名,csv>的格式，用于后续分流,为了实现change流，本来需要数据格式为row或者rowData 参考 {@linkplain
 *     DorisDynamicOutputFormat#addBatch(Object)}之后，其实doris sink拿到rowData也会转为string或者map交给
 *     streamLoad来处理，因此此处我们可以直接转为string，并且参考他的删除标记的做法来实现对应增删逻辑 本质上是：跳过了flink内部的rowKind转化流程，直接转为doris
 *     sink需要的格式
 *     <p>时区问题也需要在此处（即mysql cdc解析函数）处理，因为我们使用自定义函数，我们需要自己处理时区转换逻辑
 *     <p>2022年10月19日更新:在此版本的解析过程中,加入了对ddl语句的解析,拿到ddl之后,尝试转化为doris的ddl语句,并到doris中执行.
 *     后续还会将拿到的ddl语句都存储在一张表中做记录留存.执行代码见本类中新增方法:{@linkplain
 *     JsonDebeziumDeserializationSchema#syncDdlOnDoris(Struct)}
 */
public class JsonDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<Tuple2<String, String>> {

    private static final Logger LOG =
            LoggerFactory.getLogger(JsonDebeziumDeserializationSchema.class);
    private static final String DORIS_DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    private static final String DELETE_SIGN = "1";
    private static final FastDateFormat NORMAL_DATE_FORMATTER =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static final FastDateFormat KAFKA_DATE_TIME_FORMATTER =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT+0:00"));
    public static final String KEY_ISDDL = "__isDdl__";
    public static final String KEY_COLUMNS = "__columns__";
    public static final String KEY_COLUMNS_CHANGE = "__columnsChange__";
    public static final String KEY_NEW_SINK = "__newSink__";
    private static final String ADD_COLUMN_EXAMPLE = "alter table %s add column %s %s after %s";
    private static final Pattern PATTERN_ADD =
            Pattern.compile("(?i)(alter table )(.)+?( add column )(.*)");
    private static final Pattern PATTERN_DROP =
            Pattern.compile("(?i)(alter table )(.)+?( drop column )(.*)");
    private static final Pattern PATTERN_MODIFY =
            Pattern.compile("(?i)(alter table )(.*)( modify | modify column )(.*)");
    private static final Pattern PATTERN_CHANGE =
            Pattern.compile("(?i)(alter table )(.*)( change )(\\S* )(\\S*)(.*)");
    private static final Pattern PATTERN_CREATE =
            Pattern.compile("(?i)(create table)(.*)(\\()(.*)");
    private static final Pattern PATTERN_VARCHAR = Pattern.compile("(?i)(varchar\\()(\\d*)(\\))");
    private final ParameterTool pro;
    private final String ddlRecordTable;

    public JsonDebeziumDeserializationSchema(ParameterTool pro) {
        this.pro = pro;
        this.ddlRecordTable = pro.getRequired("ddlRecordTable");
    }

    private static String parseDeleteSign(String operation) {
        if (Envelope.Operation.DELETE.code().equals(operation)) {
            return "1";
        } else {
            return "0";
        }
    }

    private static void executeOnDoris(String dorisDdl, ParameterTool pro) throws SQLException {
        String url = "jdbc:mysql://%s:%s";
        String dorisUrl =
                String.format(url, pro.getRequired("sinkHostname"), pro.getInt("sinkPort", 3306));

        try (Connection dorisCon =
                DriverManager.getConnection(
                        dorisUrl,
                        pro.getRequired("sinkUsername"),
                        pro.getRequired("sinkPassword"))) {
            QueryRunner queryRunner = new QueryRunner();
            queryRunner.execute(dorisCon, dorisDdl);
            LOG.info("execute doris ddl successfully");
        }
    }

    private static String resetVarcharLength(String dorisDdl) {
        Matcher m2 = PATTERN_VARCHAR.matcher(dorisDdl);
        while (m2.find()) {
            String group = m2.group();
            String l1 = m2.group(2);
            int l2 = Integer.parseInt(l1) * 3;
            dorisDdl = dorisDdl.replace(group, "varchar(" + l2 + ")");
        }
        return dorisDdl;
    }

    private static String getColumnsString(List<ColumnInfo> columns) {
        return columns.stream()
                .map(columnInfo -> quote(columnInfo.getName()))
                .reduce((s, s2) -> s + "," + s2)
                .orElse(null);
    }

    private static List<ColumnInfo> getColumnInfo(JSONObject jsonObject) {
        JSONArray tableChanges = jsonObject.getJSONArray("tableChanges");
        return tableChanges
                .getJSONObject(0)
                .getJSONObject("table")
                .getList("columns", ColumnInfo.class);
    }

    private JSONObject syncDdlOnDoris(Struct value) {
        // 解析操作,并且同步到doris,此处需要开启到doris得jdbc连接
        Struct source = value.getStruct("source");
        String db = source.getString("db");
        String table = source.getString("table");
        long time = source.getInt64("ts_ms");
        String dorisTable = pro.get("sinkDatabase") + "." + db + "_" + table;

        String historyRecord = value.getString("historyRecord");
        JSONObject historyJson = JSON.parseObject(historyRecord);
        String ddl = historyJson.getString("ddl");
        return translateAndExecuteDdl(ddl, dorisTable, historyJson, db, table, time);
    }

    private JSONObject translateAndExecuteDdl(
            String ddl,
            String dorisTable,
            JSONObject historyJson,
            String db,
            String table,
            long time) {

        JSONObject r = new JSONObject();
        // 执行情况的一些指标 初始化
        r.put("isExecute", 0);
        r.put("isSucceed", 0);
        r.put("columnsStatus", 0);
        // 其他信息
        String format = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");
        r.put("time", format);
        r.put("mysqlDb", db);
        r.put("mysqlTable", table);
        r.put("dorisTable", dorisTable);
        r.put("mysqlDdl", ddl);
        r.put("host", pro.getRequired("hostname"));
        r.put("isFromCdc", "1");
        r.put(KEY_ISDDL, "");

        try {
            // 通过解析器来做解析,去除其他符合的影响
            ddl = CCJSqlParserUtil.parse(ddl).toString();
            LOG.info("get ddl from mysql:{}", ddl);

            if (PATTERN_ADD.matcher(ddl).matches()) {
                execute4AlterColumn(
                        historyJson, r, PATTERN_ADD.matcher(ddl), dorisTable, false, true);
            } else if (PATTERN_DROP.matcher(ddl).matches()) {
                execute4AlterColumn(
                        historyJson, r, PATTERN_DROP.matcher(ddl), dorisTable, false, true);
            } else if (PATTERN_MODIFY.matcher(ddl).matches()) {
                ddl = ddl.replaceAll("(?i)(modify|modify column)", "modify column");
                execute4AlterColumn(
                        historyJson, r, PATTERN_MODIFY.matcher(ddl), dorisTable, false, false);
            } else if (PATTERN_CHANGE.matcher(ddl).matches()) {
                // doris不支持修改列名,方案:doris新增同名列
                execute4AlterColumn(
                        historyJson, r, PATTERN_CHANGE.matcher(ddl), dorisTable, true, false);
            } else if (PATTERN_CREATE.matcher(ddl).matches()) {
                execute4CreateTable(historyJson, r, dorisTable);
            } else {
                // 其他 可以不处理,但是要写入ddl记录表中
                r.put("note", "only for record");
            }
        } catch (SQLException | JSQLParserException e) {
            LOG.error("execute ddl on doris failed,source mysql ddl:{}", ddl);
            LOG.error("execute failed on doris,error message:{}", e.getMessage());
            r.put("note", e.getMessage());
        }
        return r;
    }

    private void execute4CreateTable(JSONObject historyJson, JSONObject r, String dorisTable)
            throws SQLException {
        LOG.info("get create table ddl,to create sink table on doris");

        JSONArray tableChanges = historyJson.getJSONArray("tableChanges");
        JSONObject table = tableChanges.getJSONObject(0).getJSONObject("table");
        List<ColumnInfo> columnInfoList = table.getList("columns", ColumnInfo.class);
        List<String> primaryKey = table.getList("primaryKeyColumnNames", String.class);
        int priKeyCount = 0;
        ArrayList<String> columns = new ArrayList<>();
        for (ColumnInfo info : columnInfoList) {
            String name = info.getName();
            String type = info.getTypeName();
            // type需要处理
            if (type.contains("BIGINT")) {
                type = "bigint";
            } else if (type.contains("INT")) {
                type = "int";
            } else if ("VARCHAR".equals(type)) {
                int num = info.getLength() * 3;
                type = type + "(" + num + ")";
            } else if (type.contains("JSON")
                    || type.contains("TEXT")
                    || type.contains("ENUM")
                    || type.contains("BIT")) {
                type = "string";
            } else if ("TIMESTAMP".equals(type)) {
                type = "datetime";
            } else if ("DECIMAL".equals(type)) {
                int length = info.getLength();
                int scale = info.getScale();
                type = "DECIMAL(" + length + "," + scale + ")";
            }

            String columnInfo = String.format("`%s` %s", name, type);
            if (primaryKey.contains(name)) {
                priKeyCount++;
                columns.add(priKeyCount - 1, columnInfo);
            } else {
                columns.add(columnInfo);
            }
        }
        StringBuilder sb = new StringBuilder();
        String v1 = String.format("create table %s ( ", dorisTable);
        StringBuilder ddl = sb.append(v1);
        for (int i = 0; i < columns.size() - 1; i++) {
            ddl.append(columns.get(i)).append(",");
        }
        ddl.append(columns.get(columns.size() - 1)).append(" )");

        String s =
                primaryKey.stream()
                        .map(s1 -> "`" + s1 + "`")
                        .reduce((s12, s2) -> s12 + "," + s2)
                        .orElse(null);
        if (s == null) {
            throw new NullPointerException(
                    "primary key is null,please check table ddl for :" + "mysqlTable");
        }
        String v2 =
                String.format(
                        "ENGINE=OLAP UNIQUE KEY(%s) DISTRIBUTED BY HASH(`%s`) BUCKETS 3",
                        s, primaryKey.get(0));
        ddl.append(v2);

        LOG.info("to create table on doris,ddl :{}", ddl);

        String columnsString = getColumnsString(columnInfoList);
        r.put(KEY_COLUMNS, columnsString);
        r.put(KEY_COLUMNS_CHANGE, "");
        r.put(KEY_NEW_SINK, "");
        r.put("isExecute", 1);
        r.put("columnsStatus", 2);
        executeOnDoris(ddl.toString(), pro);
        r.put("isSucceed", 1);
    }

    private void execute4AlterColumn(
            JSONObject historyJson,
            JSONObject r,
            Matcher matcher,
            String dorisTable,
            boolean isChange,
            boolean needNewColumns)
            throws SQLException {
        // 加字段,初步认为 简单增删字段的ddl在mysql和doris中是语法一致得
        String dorisDdl;
        if (isChange) {
            // 当关键字是change的时候,分两种情况
            // 1.修改列名
            // 2.列名不变,修改类型等
            matcher.reset();
            matcher.matches();
            String oldColumn = matcher.group(4).replace(" ", "");
            String newColumn = matcher.group(5).replace(" ", "");
            String msg = matcher.group(6);
            if (oldColumn.equals(newColumn)) {
                // 列名不变,修改类型,
                dorisDdl = matcher.replaceAll("$1" + dorisTable + " modify column " + "$5$6");
            } else {
                // 修改了列名,方案是:新增一个列,所以需要更改列参数
                needNewColumns = true;
                dorisDdl = String.format(ADD_COLUMN_EXAMPLE, dorisTable, newColumn, msg, oldColumn);
            }
        } else {
            dorisDdl = matcher.replaceAll("$1" + dorisTable + "$3$4");
        }
        dorisDdl = resetVarcharLength(dorisDdl);
        r.put("dorisDdl", dorisDdl);
        LOG.info("ADD COLUMN,translate to doris ddl:{}", dorisDdl);

        if (needNewColumns) {
            List<ColumnInfo> columns = getColumnInfo(historyJson);
            String columnsString = getColumnsString(columns);
            if (columnsString == null) {
                r.put("columnsStatus", 1);
                LOG.error("get new columns list failed,will not execute ddl on doris");
            } else {
                LOG.info("get new columns list:{}", columnsString);
                r.put("columnsStatus", 2);
                r.put("isExecute", 1);
                executeOnDoris(dorisDdl, pro);
                r.put("isSucceed", 1);
                r.put(KEY_COLUMNS, columnsString);
                r.put(KEY_COLUMNS_CHANGE, "");
            }
        } else {
            r.put("isExecute", 1);
            executeOnDoris(dorisDdl, pro);
            r.put("isSucceed", 1);
        }
    }

    @Override
    public void deserialize(
            SourceRecord sourceRecord, Collector<Tuple2<String, String>> collector) {
        Struct value = (Struct) sourceRecord.value();
        Struct binlogSource = value.getStruct("source");
        String database = binlogSource.getString("db");
        String tableName = binlogSource.getString("table");
        String mysqlTable = database + "." + tableName;
        // 根据是否具有op分类
        Field op = value.schema().field("op");
        if (op == null) {
            // 此时 我们认为这不是一条数据信息,而是一条ddl
            JSONObject r = syncDdlOnDoris(value);
            if (r.containsKey(KEY_COLUMNS_CHANGE)) {
                // 当需要改变列,重置streamLoad的时候才发送到相关的sink中
                collector.collect(new Tuple2<>(mysqlTable, r.toString()));
            }
            // 后续都要发送到记录表中
            r.remove(KEY_ISDDL);
            r.remove(KEY_COLUMNS);
            collector.collect(new Tuple2<>(ddlRecordTable, r.toString()));

            return;
        }

        String opString = Envelope.Operation.forCode(value.getString(op.name())).code();
        String deleteSign = parseDeleteSign(opString);
        Struct valueStruct = null;
        try {
            if (DELETE_SIGN.equals(deleteSign)) {
                valueStruct = value.getStruct("before");
            } else {
                valueStruct = value.getStruct("after");
            }
        } catch (DataException ignored) {
            LOG.info("get msg,but have no before or after struct,skip it.");
        }

        if (valueStruct != null) {
            HashMap<String, String> result = new HashMap<>(16);
            result.put(DORIS_DELETE_SIGN, deleteSign);

            Schema afterSchema = valueStruct.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object fieldValue = convertTime(valueStruct.get(field), field);
                String single = fieldValue != null ? fieldValue.toString() : null;
                result.put(field.name(), single);
            }
            collector.collect(new Tuple2<>(mysqlTable, JSON.toJSONString(result)));
        }
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return null;
    }

    private Object convertTime(Object object, Field field) {
        Schema schema = field.schema();
        if (schema.name() == null) {
            return object;
        }

        switch (schema.name()) {
                // date
            case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                Date date1 = (Date) object;
                String r1;
                try {
                    r1 = NORMAL_DATE_FORMATTER.format(date1).substring(0, 10);
                } catch (Exception e) {
                    r1 = "1970-01-01";
                }
                return r1;
                // datetime
            case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                Date date2 = (Date) object;
                String r2;
                try {
                    r2 = KAFKA_DATE_TIME_FORMATTER.format(date2);
                } catch (Exception e) {
                    r2 = "1970-01-01 00:00:00";
                }
                return r2;
                // timestamp
            case io.debezium.time.ZonedTimestamp.SCHEMA_NAME:
                String parseStr =
                        ((String) object).replace("T", " ").replace("Z", "").substring(0, 19);
                Date re;
                String r3;
                try {
                    re = KAFKA_DATE_TIME_FORMATTER.parse(parseStr);
                    r3 = NORMAL_DATE_FORMATTER.format(re);
                } catch (Exception e) {
                    r3 = "1970-01-01 00:00:00";
                }
                return r3;
            default:
                return object;
        }
    }
}
