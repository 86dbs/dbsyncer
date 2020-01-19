package org.dbsyncer.common.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public abstract class JsonUtil {

	private static final ObjectMapper mapper = new ObjectMapper();

	public static String objToJson(Object obj) {
		try {
			// 将对象转换成json
			return mapper.writeValueAsString(obj);
		} catch (JsonGenerationException e) {
		} catch (JsonMappingException e) {
		} catch (IOException e) {
		}
		return null;
	}

	public static <T> T jsonToObj(String json, Class<T> valueType) {
		try {
			return (T) mapper.readValue(json, valueType);
		} catch (Exception e) {
		}
		return null;
	}

}