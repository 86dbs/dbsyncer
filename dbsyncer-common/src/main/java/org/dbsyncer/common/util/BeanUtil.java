package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public abstract class BeanUtil {

    public static void mapToBean(Map<String, Object> map, Object instance) {
        try {
            Class<?> clazz = instance.getClass();
            for (Map.Entry<String, Object> eachMap : map.entrySet()) {
                String property = eachMap.getKey();
                Object value = eachMap.getValue();

                String setMethod = "set" + property.substring(0, 1).toUpperCase() + property.substring(1);
                Field field = getField(property, clazz);
                if (field == null) {
                    continue;
                }
                Class<?> fType = field.getType();
                Object newValue = convert(value, fType);
                clazz.getMethod(setMethod, fType).invoke(instance, newValue);
            }
        } catch (Exception e) {
            throw new CommonException(e);
        }
    }

    public static Map beanToMap(Object object) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        Field[] field = object.getClass().getDeclaredFields();
        Map map = new HashMap();
        for (Field fi : field) {
            String property = fi.getName();
            String getMe = "get" + property.substring(0, 1).toUpperCase() + property.substring(1);
            Object obj = object.getClass().getMethod(getMe).invoke(object);
            map.put(property, obj);
        }
        return map;
    }

    private static Object convert(Object value, Class<?> fType) {
        if (value == null) {
            return null;
        }

        // 如果类型已经匹配，直接返回
        if (fType.isInstance(value)) {
            return value;
        }

        String targetTypeName = fType.getName();

        // 字符串转数字和布尔
        if (value instanceof String) {
            String strValue = (String) value;
            if (StringUtil.isBlank(strValue)) {
                return getDefaultValue(fType);
            }

            // String -> Long
            if (Long.class.getName().equals(targetTypeName) || long.class.getName().equals(targetTypeName)) {
                try {
                    return Long.parseLong(strValue.trim());
                } catch (NumberFormatException e) {
                    return NumberUtil.toLong(strValue, 0L);
                }
            }

            // String -> Integer
            if (Integer.class.getName().equals(targetTypeName) || int.class.getName().equals(targetTypeName)) {
                try {
                    return Integer.parseInt(strValue.trim());
                } catch (NumberFormatException e) {
                    return NumberUtil.toInt(strValue, 0);
                }
            }

            // String -> Short
            if (Short.class.getName().equals(targetTypeName) || short.class.getName().equals(targetTypeName)) {
                try {
                    return Short.parseShort(strValue.trim());
                } catch (NumberFormatException e) {
                    return (short) NumberUtil.toInt(strValue, 0);
                }
            }

            // String -> Byte
            if (Byte.class.getName().equals(targetTypeName) || byte.class.getName().equals(targetTypeName)) {
                try {
                    return Byte.parseByte(strValue.trim());
                } catch (NumberFormatException e) {
                    return (byte) NumberUtil.toInt(strValue, 0);
                }
            }

            // String -> Float
            if (Float.class.getName().equals(targetTypeName) || float.class.getName().equals(targetTypeName)) {
                try {
                    return Float.parseFloat(strValue.trim());
                } catch (NumberFormatException e) {
                    return 0.0f;
                }
            }

            // String -> Double
            if (Double.class.getName().equals(targetTypeName) || double.class.getName().equals(targetTypeName)) {
                try {
                    return Double.parseDouble(strValue.trim());
                } catch (NumberFormatException e) {
                    return 0.0d;
                }
            }

            // String -> Boolean
            if (Boolean.class.getName().equals(targetTypeName) || boolean.class.getName().equals(targetTypeName)) {
                String trimmed = strValue.trim().toLowerCase();
                return "true".equals(trimmed) || "1".equals(trimmed) || "yes".equals(trimmed) || "on".equals(trimmed);
            }
        }

        // 数字转字符串
        if (value instanceof Number && String.class.equals(fType)) {
            return String.valueOf(value);
        }

        // 布尔转字符串
        if (value instanceof Boolean && String.class.equals(fType)) {
            return String.valueOf(value);
        }

        // 数字类型之间的转换
        if (value instanceof Number) {
            Number num = (Number) value;

            // Number -> Long
            if (Long.class.getName().equals(targetTypeName) || long.class.getName().equals(targetTypeName)) {
                return num.longValue();
            }

            // Number -> Integer
            if (Integer.class.getName().equals(targetTypeName) || int.class.getName().equals(targetTypeName)) {
                return num.intValue();
            }

            // Number -> Short
            if (Short.class.getName().equals(targetTypeName) || short.class.getName().equals(targetTypeName)) {
                return num.shortValue();
            }

            // Number -> Byte
            if (Byte.class.getName().equals(targetTypeName) || byte.class.getName().equals(targetTypeName)) {
                return num.byteValue();
            }

            // Number -> Float
            if (Float.class.getName().equals(targetTypeName) || float.class.getName().equals(targetTypeName)) {
                return num.floatValue();
            }

            // Number -> Double
            if (Double.class.getName().equals(targetTypeName) || double.class.getName().equals(targetTypeName)) {
                return num.doubleValue();
            }
        }

        // 布尔类型之间的转换
        if (value instanceof Boolean) {
            if (Boolean.class.getName().equals(targetTypeName) || boolean.class.getName().equals(targetTypeName)) {
                return value;
            }
        }

        return value;
    }

    /**
     * 获取基本类型的默认值
     */
    private static Object getDefaultValue(Class<?> type) {
        if (type.isPrimitive()) {
            if (type == boolean.class) {
                return false;
            }
            if (type == byte.class) {
                return (byte) 0;
            }
            if (type == short.class) {
                return (short) 0;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            if (type == float.class) {
                return 0.0f;
            }
            if (type == double.class) {
                return 0.0d;
            }
            if (type == char.class) {
                return '\u0000';
            }
        }
        return null;
    }

    private static Field getField(String property, Class<?> obj) {
        if (Object.class.getName().equals(obj.getName())) {
            return null;
        }
        Field[] field = obj.getDeclaredFields();
        for (Field f : field) {
            if (f.getName().equals(property)) {
                return f;
            }
        }
        Class<?> parent = obj.getSuperclass();
        if (parent != null) {
            return getField(property, parent);
        }
        return null;
    }
}
