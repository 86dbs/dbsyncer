/**
 * DBSyncer Copyright 2020-2025 All Rights Reserved.
 */
package org.dbsyncer.biz;

import org.dbsyncer.biz.vo.ParamConfigVo;
import org.dbsyncer.sdk.enums.ParamKeyEnum;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 参数配置服务
 * 
 * @author AE86
 * @version 1.0.0
 * @date 2025/01/08
 */
@Service
public class ParamConfigService {

    /**
     * 获取所有可用参数配置
     */
    public List<ParamConfigVo> getAllParamConfigs() {
        return Arrays.stream(ParamKeyEnum.values())
                .map(this::convertToVo)
                .collect(Collectors.toList());
    }

    /**
     * 验证参数键值对
     */
    public void validateParams(Map<String, String> params) {
        if (params == null) {
            return;
        }

        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            ParamKeyEnum paramEnum = ParamKeyEnum.getByKey(key);
            if (paramEnum != null) {
                // 验证参数类型
                validateParamType(paramEnum, value);
            }
        }
    }

    /**
     * 转换枚举为VO对象
     */
    private ParamConfigVo convertToVo(ParamKeyEnum paramEnum) {
        ParamConfigVo vo = new ParamConfigVo();
        vo.setKey(paramEnum.getKey());
        vo.setName(paramEnum.getName());
        vo.setType(paramEnum.getType());
        vo.setDescription(paramEnum.getDescription());
        return vo;
    }

    /**
     * 验证参数类型
     */
    private void validateParamType(ParamKeyEnum paramEnum, String value) {
        if (value == null || value.trim().isEmpty()) {
            return;
        }

        String type = paramEnum.getType();
        try {
            switch (type.toLowerCase()) {
                case "int":
                case "integer":
                    Integer.parseInt(value);
                    break;
                case "long":
                    Long.parseLong(value);
                    break;
                case "boolean":
                case "bool":
                    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                        throw new IllegalArgumentException("参数 " + paramEnum.getName() + " 的值必须是 true 或 false");
                    }
                    break;
                case "double":
                    Double.parseDouble(value);
                    break;
                case "float":
                    Float.parseFloat(value);
                    break;
                case "string":
                default:
                    // 字符串类型不需要特殊验证
                    break;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("参数 " + paramEnum.getName() + " 的值格式不正确，应为 " + type + " 类型");
        }
    }
}