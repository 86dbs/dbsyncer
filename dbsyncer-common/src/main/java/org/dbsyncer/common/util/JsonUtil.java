package org.dbsyncer.common.util;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dbsyncer.common.CommonException;

import java.io.IOException;

public abstract class JsonUtil {

	private static final ObjectMapper mapper = new ObjectMapper();

	public static String objToJson(Object obj) {
		try {
			// 将对象转换成json
			return mapper.writeValueAsString(obj);
		} catch (JsonGenerationException e) {
			throw new CommonException(e);
		} catch (JsonMappingException e) {
			throw new CommonException(e);
		} catch (IOException e) {
			throw new CommonException(e);
		}
	}

	public static <T> T jsonToObj(String json, Class<T> valueType) {
		try {
			return (T) mapper.readValue(json, valueType);
		} catch (Exception e) {
			throw new CommonException(e);
		}
	}

}