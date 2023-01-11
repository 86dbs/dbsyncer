import org.apache.commons.io.FileUtils;
import org.dbsyncer.common.model.AbstractConnectorConfig;
import org.dbsyncer.common.util.JsonUtil;
import org.dbsyncer.connector.enums.ConnectorEnum;
import org.dbsyncer.parser.model.Connector;
import org.dbsyncer.parser.model.Mapping;
import org.dbsyncer.parser.model.TableGroup;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2019/10/9 23:46
 */
public class ConnectorParserTest {

    @Test
    public void testConnector() throws IOException {
        String json = readJson("Connector.json");
        System.out.println(json);

        // 解析基本信息
        Map conn = JsonUtil.parseMap(json);
        Map config = (Map) conn.remove("config");
        Connector connector = JsonUtil.jsonToObj(conn.toString(), Connector.class);

        // 解析配置
        String connectorType = (String) config.get("connectorType");
        Class<?> configClass = ConnectorEnum.getConfigClass(connectorType);
        Object obj = JsonUtil.jsonToObj(config.toString(), configClass);
        connector.setConfig((AbstractConnectorConfig) obj);
        System.out.println(connector);
    }

    @Test
    public void testMapping() throws IOException {
        String json = readJson("Mapping.json");
        System.out.println(json);

        // 解析基本信息
        Mapping mapping = JsonUtil.jsonToObj(json, Mapping.class);
        System.out.println(mapping);
    }

    @Test
    public void testTableGroup() throws IOException {
        String json = readJson("TableGroup.json");
        System.out.println(json);
        // 解析基本信息
        TableGroup tableGroup = JsonUtil.jsonToObj(json, TableGroup.class);
        System.out.println(tableGroup);
    }

    /**
     * 读取JSON文件
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    private String readJson(String fileName) throws IOException {
        ClassLoader loader = this.getClass().getClassLoader();
        URL fileURL = loader.getResource(fileName);
        if (null != fileURL) {
            String filePath = fileURL.getFile();
            File file = new File(filePath);
            return FileUtils.readFileToString(file, "UTF-8");
        }
        return "";
    }

}