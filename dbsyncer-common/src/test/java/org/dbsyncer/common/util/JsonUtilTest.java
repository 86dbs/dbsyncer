package org.dbsyncer.common.util;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.annotation.JSONField;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * 测试FastJSON2对私有属性的序列化行为
 */
public class JsonUtilTest {

    /**
     * 测试对象包含公有和私有字段
     */
    public static class TestEntity {
        @JSONField(serialize = false)
        private String privateField = "privateValue";
        private int privateIntField = 100;
        public String publicField = "publicValue";
        private String privateFieldNotSerialized = "notSerialized";
        
        // 私有方法，不会被序列化
        private String getPrivateFieldValue() {
            return "privateMethodValue";
        }
        
        // 公有方法，不会被序列化（除非是getter）
        public String getPublicFieldValue() {
            return "publicMethodValue";
        }
        
        // 标准getter方法，会被序列化
        public String getPrivateField() {
            return privateField;
        }
        
        public int getPrivateIntField() {
            return privateIntField;
        }
        
        // 没有对应字段的getter方法
        public String getCustomValue() {
            return "customValue";
        }
    }

    @Test
    public void testObjToJsonWithPrivateFields() {
        TestEntity entity = new TestEntity();
        String json = JsonUtil.objToJson(entity);
        System.out.println("Serialized JSON: " + json);

        // 解析回Map检查包含的字段
        Map<String, Object> parsed = JsonUtil.jsonToObj(json, Map.class);

        // 验证私有字段通过getter方法被序列化
        Assert.assertFalse("应该包含privateField字段", parsed.containsKey("privateField"));

        // 验证私有int字段通过getter方法被序列化
        Assert.assertTrue("应该包含privateIntField字段", parsed.containsKey("privateIntField"));
        Assert.assertEquals("privateIntField值应该正确", 100, parsed.get("privateIntField"));

        // 验证公有字段被序列化
        Assert.assertTrue("应该包含publicField字段", parsed.containsKey("publicField"));
        Assert.assertEquals("publicField值应该正确", "publicValue", parsed.get("publicField"));

        // 验证通过getter方法返回的自定义值被序列化
        Assert.assertTrue("应该包含customValue字段", parsed.containsKey("customValue"));
        Assert.assertEquals("customValue值应该正确", "customValue", parsed.get("customValue"));

        // 验证私有字段本身没有被直接序列化（没有getter方法）
        Assert.assertFalse("不应该包含privateFieldNotSerialized字段", parsed.containsKey("privateFieldNotSerialized"));
    }
}