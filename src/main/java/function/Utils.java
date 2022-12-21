package function;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.AbstractListHandler;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author wfs
 */
public class Utils {

    private Utils() {
        throw new IllegalStateException("utils class");
    }

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static Map<String, String> createDorisTables(ParameterTool pro) throws SQLException {
        String url = "jdbc:mysql://%s:%s";
        String mysqlUrl = String.format(url, pro.getRequired("hostname"), pro.getInt("port", 3306));
        String dorisUrl =
                String.format(url, pro.getRequired("sinkHostname"), pro.getInt("sinkPort", 3306));

        try (Connection mysqlCon =
                        DriverManager.getConnection(
                                mysqlUrl,
                                pro.getRequired("username"),
                                pro.getRequired("password"));
                Connection dorisCon =
                        DriverManager.getConnection(
                                dorisUrl,
                                pro.getRequired("sinkUsername"),
                                pro.getRequired("sinkPassword"))) {

            LOG.info("Read list of selected databases");
            String sinkDatabase = pro.getRequired("sinkDatabase");
            String databaseList = pro.getRequired("databaseList");
            String[] v1 = databaseList.split(",");
            String tableList = pro.getRequired("tableList");
            String[] v2 = tableList.split(",");
            QueryRunner queryRunner = new QueryRunner();

            List<String> databases =
                    queryRunner
                            .query(
                                    mysqlCon,
                                    "SHOW DATABASES",
                                    new AbstractListHandler<String>() {
                                        @Override
                                        protected String handleRow(ResultSet rs)
                                                throws SQLException {
                                            String database = rs.getString(1);
                                            for (String s : v1) {
                                                if (Pattern.matches(s, database)) {
                                                    return database;
                                                }
                                            }
                                            return null;
                                        }
                                    })
                            .stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            LOG.info("select databases:{}", databases);

            LOG.info("Read list of selected table");
            ArrayList<String> mysqlTables = new ArrayList<>();
            for (String dbName : databases) {
                mysqlTables.addAll(
                        queryRunner
                                .query(
                                        mysqlCon,
                                        "SHOW FULL TABLES IN "
                                                + quote(dbName)
                                                + " where Table_Type = 'BASE TABLE'",
                                        new AbstractListHandler<String>() {
                                            @Override
                                            protected String handleRow(ResultSet rs)
                                                    throws SQLException {
                                                String table = dbName + "." + rs.getString(1);

                                                for (String s : v2) {
                                                    if (Pattern.matches(s, table)) {
                                                        return table;
                                                    }
                                                }
                                                return null;
                                            }
                                        })
                                .stream()
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList()));
            }
            LOG.info("select tables:{}", mysqlTables);

            LOG.info("check or create doris table");
            for (String mysqlTable : mysqlTables) {
                String[] split = mysqlTable.split("\\.");
                String dbName = split[0];
                String tableName = split[1];
                String sinkTable = dbName + "_" + tableName;
                String sinkFullName = sinkDatabase + "." + sinkTable;

                LOG.info("mysql table: {} -> doris table: {}", mysqlTable, sinkFullName);
                queryRunner.query(
                        dorisCon,
                        String.format(
                                "select count(1) from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME ='%s'",
                                sinkDatabase, sinkTable),
                        new AbstractListHandler<String>() {
                            @Override
                            protected String handleRow(ResultSet rs) throws SQLException {
                                int count = rs.getInt(1);
                                if (count == 0) {
                                    LOG.info(
                                            "table: {} is not found in doris,to create it",
                                            sinkFullName);
                                    createDorisTable(
                                            queryRunner,
                                            dorisCon,
                                            mysqlCon,
                                            mysqlTable,
                                            sinkDatabase,
                                            sinkTable,
                                            pro.getBoolean("keepNullSet", false));
                                    LOG.info("create table: {} successfully", sinkFullName);
                                } else {
                                    LOG.info(
                                            "table: {} is already created in doris,continue...",
                                            sinkFullName);
                                }
                                return null;
                            }
                        });
            }

            LOG.info("get map for <mysqlTableName,tableColumns>");
            int size = (int) (mysqlTables.size() / 0.75F + 1.0F);
            Map<String, String> result = new HashMap<>(size);
            for (String mysqlTable : mysqlTables) {
                String[] split = mysqlTable.split("\\.");
                String dbName = split[0];
                String tableName = split[1];
                String column =
                        queryRunner.query(
                                mysqlCon,
                                String.format(
                                        "SELECT GROUP_CONCAT(concat(\"`\",COLUMN_NAME,\"`\") SEPARATOR \",\") as columns FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                                        dbName, tableName),
                                rs ->
                                        rs.next()
                                                ? rs.getString(1) + ",__DORIS_DELETE_SIGN__"
                                                : null);

                result.put(mysqlTable, column);
            }
            LOG.info("check and create doris table successfully,close connections");

            LOG.info("##############################################################");
            return result;
        }
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    /**
     * 此处与{@linkplain JsonDebeziumDeserializationSchema#execute4CreateTable}不同,因为拿到的信息源不同,所以处理逻辑也不同
     * 但是两者对类型的转化逻辑相同
     */
    public static void createDorisTable(
            QueryRunner queryRunner,
            Connection dorisCon,
            Connection mysqlCon,
            String mysqlTable,
            String sinkDatabase,
            String sinkTable,
            boolean keepNullSet)
            throws SQLException {
        String ddl =
                queryRunner.query(
                        mysqlCon,
                        "show full columns from " + mysqlTable,
                        rs -> {
                            ArrayList<String> columns = new ArrayList<>();
                            ArrayList<String> priKeyName = new ArrayList<>();
                            while (rs.next()) {
                                String name = rs.getString(1);
                                String type = rs.getString(2);
                                // type需要处理
                                if (type.contains("bigint")) {
                                    type = "bigint";
                                } else if (type.contains("int")) {
                                    type = "int";
                                } else if (type.contains("varchar")) {
                                    String num = type.replace("varchar", "").replaceAll("[()]", "");
                                    int num2 = Integer.parseInt(num) * 3;
                                    type = type.replaceAll("\\d+", num2 + "");
                                } else if (type.contains("json")
                                        || type.contains("text")
                                        || type.contains("enum")
                                        || type.contains("bit")) {
                                    type = "string";
                                } else if (type.contains("timestamp")) {
                                    type = "datetime";
                                } else if (type.contains("double(")) {
                                    type = type.replace("double", "DECIMAL");
                                }

                                String isNull = rs.getString(4);
                                String key = rs.getString(5);
                                String comment = rs.getString(9).replace("'", "");

                                String columnInfo =
                                        String.format(
                                                "`%s` %s %s COMMENT '%s'",
                                                name,
                                                type,
                                                (keepNullSet && "NO".equals(isNull)
                                                        ? " NOT NULL "
                                                        : " "),
                                                comment);

                                if ("PRI".equals(key)) {
                                    priKeyName.add(name);
                                    columns.add(priKeyName.size() - 1, columnInfo);
                                } else {
                                    columns.add(columnInfo);
                                }
                            }

                            StringBuilder sb = new StringBuilder();
                            String v1 =
                                    String.format(
                                            "create table %s ( ", sinkDatabase + "." + sinkTable);
                            StringBuilder ddl1 = sb.append(v1);
                            for (int i = 0; i < columns.size() - 1; i++) {
                                ddl1.append(columns.get(i)).append(",");
                            }
                            ddl1.append(columns.get(columns.size() - 1)).append(" )");

                            String s =
                                    priKeyName.stream()
                                            .map(s1 -> "`" + s1 + "`")
                                            .reduce((s12, s2) -> s12 + "," + s2)
                                            .orElse(null);
                            if (s == null) {
                                throw new NullPointerException(
                                        "primary key is null,please check table ddl for :"
                                                + mysqlTable);
                            }
                            String v2 =
                                    String.format(
                                            "ENGINE=OLAP UNIQUE KEY(%s) DISTRIBUTED BY HASH(`%s`) BUCKETS 3",
                                            s, priKeyName.get(0));
                            ddl1.append(v2);
                            return ddl1.toString();
                        });
        queryRunner.update(dorisCon, ddl);
    }
}
