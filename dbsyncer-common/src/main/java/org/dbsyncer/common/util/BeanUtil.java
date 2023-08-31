package org.dbsyncer.common.util;

import org.dbsyncer.common.CommonException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public abstract class BeanUtil {

    public static Object mapToBean(Map<String, String> map, Object instance) {
        try {
            Class<?> clazz = instance.getClass();
            for (Map.Entry<String, String> eachMap : map.entrySet()) {
                String property = eachMap.getKey();
                String value = eachMap.getValue();

                String setMethod = "set" + property.substring(0, 1).toUpperCase() + property.substring(1);
                Field field = getField(property, clazz);
                Class<?> fType = field.getType();
                Object newValue = convert(value, fType);
                clazz.getMethod(setMethod, fType).invoke(instance, newValue);
            }
        } catch (Exception e) {
            throw new CommonException(e);
        }
        return instance;
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

    private static Object convert(String value, Class<?> fType) {
        if (Long.class.getName().equals(fType.getName()) || long.class.getName().equals(fType.getName())) {
            return Long.parseLong(value);
        }

        if (Float.class.getName().equals(fType.getName()) || float.class.getName().equals(fType.getName())) {
            return Float.parseFloat(value);
        }

        if (Double.class.getName().equals(fType.getName()) || double.class.getName().equals(fType.getName())) {
            return Double.parseDouble(value);
        }

        if (Integer.class.getName().equals(fType.getName()) || int.class.getName().equals(fType.getName())) {
            return Integer.parseInt(value);
        }

        if (Boolean.class.getName().equals(fType.getName()) || boolean.class.getName().equals(fType.getName())) {
            return Boolean.valueOf(value);
        }
        return value;
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
