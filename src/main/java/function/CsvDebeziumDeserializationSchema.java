package function;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.doris.flink.table.DorisDynamicOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Date;
import java.util.List;
import java.util.StringJoiner;
import java.util.TimeZone;

/**
 * @author wfs 自定义一个解析函数，将数据解析为 <库表名,csv>的格式，用于后续分流,为了实现change流，本来需要数据格式为row或者rowData 参考 {@linkplain
 *     DorisDynamicOutputFormat#addBatch(Object)}之后，其实doris sink拿到rowData也会转为string或者map交给
 *     streamLoad来处理，因此此处我们可以直接转为string，并且参考他的删除标记的做法来实现对应增删逻辑 本质上是：跳过了flink内部的rowKind转化流程，直接转为doris
 *     sink需要的格式
 *     <p>时区问题也需要在此处（即mysql cdc解析函数）处理，因为我们使用自定义函数，我们需要自己处理时区转换逻辑
 */
public class CsvDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<Tuple2<String, String>> {
    private static final CharSequence FIELD_DELIMITER = ",";
    private static final String DELETE_SIGN = "1";
    private static final FastDateFormat NORMAL_DATE_FORMATTER =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static final FastDateFormat KAFKA_DATE_TIME_FORMATTER =
            FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone("GMT+0:00"));

    private static String parseDeleteSign(String operation) {
        if (Envelope.Operation.DELETE.code().equals(operation)) {
            return "1";
        } else {
            return "0";
        }
    }

    @Override
    public void deserialize(
            SourceRecord sourceRecord, Collector<Tuple2<String, String>> collector) {
        StringJoiner result = new StringJoiner(FIELD_DELIMITER);

        Struct value = (Struct) sourceRecord.value();
        Struct binlogSource = value.getStruct("source");
        String database = (String) binlogSource.get("db");
        String tableName = (String) binlogSource.get("table");

        Field op = value.schema().field("op");
        String opString =
                op == null ? null : Envelope.Operation.forCode(value.getString(op.name())).code();
        String deleteSign = parseDeleteSign(opString);

        Struct valueStruct;
        if (DELETE_SIGN.equals(deleteSign)) {
            valueStruct = value.getStruct("before");
        } else {
            valueStruct = value.getStruct("after");
        }
        if (valueStruct != null) {
            Schema afterSchema = valueStruct.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = convertTime(valueStruct.get(field), field);
                String single = afterValue != null ? afterValue.toString() : "\\N";
                result.add(single);
            }
        }
        result.add(parseDeleteSign(opString));

        collector.collect(new Tuple2<>(database + "." + tableName, result.toString()));
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
