package bean;

import static model.Constant.*;

/**
 * @author wfs
 */
public enum RunEnv {
    /*
     *   uat环境
     * */
    uat(
            KAFKA_HOST_UAT,
            KAFKA_PORT_UAT,
            ES_HOST_UAT,
            ES_PORT_UAT,
            MYSQL_HOST_UAT,
            MYSQL_USER_UAT,
            MYSQL_PASS_UAT,
            REDIS_HOST_UAT,
            CLICKHOUSE_HOST_UAT,
            CLICKHOUSE_PORT_UAT,
            CLICKHOUSE_USER_UAT,
            CLICKHOUSE_PASSWORD_UAT,
            ZOOKEEPER_HOST_UAT,
            HBASE_ZOOKEEPER_NODE_PATH_UAT,
            ZOOKEEPER_CLIENT_PORT_UAT),
    /*
     *   xdp环境
     * */
    xdp(
            KAFKA_HOST_XDP,
            KAFKA_PORT_XDP,
            ES_HOST_XDP,
            ES_PORT_XDP,
            MYSQL_HOST_XDP,
            MYSQL_USER_XDP,
            MYSQL_PASS_XDP,
            REDIS_HOST_XDP,
            CLICKHOUSE_HOST_XDP,
            CLICKHOUSE_PORT_XDP,
            CLICKHOUSE_USER_XDP,
            CLICKHOUSE_PASSWORD_XDP,
            ZOOKEEPER_HOST_XDP,
            HBASE_ZOOKEEPER_NODE_PATH_XDP,
            ZOOKEEPER_CLIENT_PORT_XDP);

    private final String kafkaHost;
    private final int kafkaPort;
    private final String esHost;
    private final int esPort;
    private final String mysqlHost;
    private final String mysqlUser;
    private final String mysqlPass;
    private final String redisHost;
    private final String clickHouseHost;
    private final int clickHousePort;
    private final String clickHouseUser;
    private final String clickHousePassword;
    private final String zookeeperHost;
    private final String hbaseZookeeperNodePath;
    private final String zookeeperClientPort;

    RunEnv(
            String kafkaHost,
            int kafkaPort,
            String esHost,
            int esPort,
            String mysqlHost,
            String mysqlUser,
            String mysqlPass,
            String redisHost,
            String clickHouseHost,
            int clickHousePort,
            String clickHouseUser,
            String clickHousePassword,
            String zookeeperHost,
            String hbaseZookeeperNodePath,
            String zookeeperClientPort) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.esHost = esHost;
        this.esPort = esPort;
        this.mysqlHost = mysqlHost;
        this.mysqlUser = mysqlUser;
        this.mysqlPass = mysqlPass;
        this.redisHost = redisHost;
        this.clickHouseHost = clickHouseHost;
        this.clickHousePort = clickHousePort;
        this.clickHouseUser = clickHouseUser;
        this.clickHousePassword = clickHousePassword;
        this.zookeeperHost = zookeeperHost;
        this.hbaseZookeeperNodePath = hbaseZookeeperNodePath;
        this.zookeeperClientPort = zookeeperClientPort;
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public int getKafkaPort() {
        return kafkaPort;
    }

    public String getEsHost() {
        return esHost;
    }

    public int getEsPort() {
        return esPort;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public String getMysqlPass() {
        return mysqlPass;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public String getClickHouseHost() {
        return clickHouseHost;
    }

    public int getClickHousePort() {
        return clickHousePort;
    }

    public String getClickHouseUser() {
        return clickHouseUser;
    }

    public String getClickHousePassword() {
        return clickHousePassword;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public String getHbaseZookeeperNodePath() {
        return hbaseZookeeperNodePath;
    }

    public String getZookeeperClientPort() {
        return zookeeperClientPort;
    }
}
