package model;

import bean.RunEnv;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * flink主函数模板
 *
 * @author wfs
 */
public class FlinkMainModel {
    public static final String ENV_XDP = "xdp";
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMainModel.class);

    /**
     * 以第一个参数作为配置文件地址，读取该配置文件，没有配置文件将退出程序
     *
     * @param args 主函数参数
     * @return 配置文件工具
     */
    public static ParameterTool getPro(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IOException("args[0] must be properties path.");
        }
        String propertiesPath = args[0];
        LOG.info("arg[0] is set as properties path: " + propertiesPath);
        return ParameterTool.fromPropertiesFile(propertiesPath);
    }

    /**
     * 以指定序号参数作为配置文件地址，读取该配置文件，没有配置文件将退出程序
     *
     * @param args 主函数参数
     * @param pos 指定参数序号为配置文件地址
     * @return 配置文件工具
     */
    public static ParameterTool getPro(String[] args, int pos) throws IOException {
        if (pos < 0 || args.length < pos + 1) {
            throw new IOException("args[pos] must be properties path.");
        }
        String propertiesPath = args[pos];
        LOG.info("arg[{}] is set as properties path: {}", pos, propertiesPath);
        return ParameterTool.fromPropertiesFile(propertiesPath);
    }

    /**
     * 将配置文件打包到jar中,通过文件名获取该配置文件
     *
     * @param flinkMainClass 主程序的class
     * @param filePath 文件名,前面带 / ,表示从根目录搜寻
     * @return 配置文件工具
     */
    public static ParameterTool getProFromJar(Class<?> flinkMainClass, String filePath)
            throws IOException {
        try {
            InputStream resourceAsStream = flinkMainClass.getResourceAsStream(filePath);
            return ParameterTool.fromPropertiesFile(resourceAsStream);
        } catch (Exception e) {
            throw new FileNotFoundException(String.format("file:%s is not found in jar", filePath));
        }
    }

    /**
     * 获取环境地址，包括：kafka，es，mysql
     *
     * @param mode uat或者xdp
     * @return 一个枚举类，包含了各个系统的地址
     */
    public static RunEnv getEnv(String mode) {
        if (ENV_XDP.equals(mode)) {
            return RunEnv.xdp;
        } else {
            return RunEnv.uat;
        }
    }
}
