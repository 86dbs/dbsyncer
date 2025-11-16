/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbsyncer.biz.vo;

import java.io.Serializable;

/**
 * Rest请求响应对象
 * 
 * @author AE86
 * @date 2017年3月30日 下午2:26:19
 * @version 1.0.0
 */
public class RestResult implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * 请求是否成功
	 */
	private boolean success;

	/**
	 * 请求成功后返回的结果数据
	 */
	private Object resultValue;

	/**
	 * 状态码
	 */
	private int status;

	/**
	 * 请求失败返回提示信息
	 * 
	 * @param resultValue
	 * @return RestResult
	 */
	public static RestResult restFail(Object resultValue) {
		return new RestResult(false, resultValue);
	}
	
	/**
	 * 请求失败返回提示信息
	 * 
	 * @param resultValue
	 * @param status 状态码
	 * @return RestResult
	 */
	public static RestResult restFail(Object resultValue, int status) {
		return new RestResult(false, resultValue, status);
	}

	/**
	 * 请求成功返回结果数据
	 * 
	 * @param resultValue
	 * @return RestResult
	 */
	public static RestResult restSuccess(Object resultValue) {
		return new RestResult(true, resultValue, 200);
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public Object getResultValue() {
		return resultValue;
	}

	public void setResultValue(Object resultValue) {
		this.resultValue = resultValue;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public RestResult(boolean success, Object resultValue) {
		super();
		this.success = success;
		this.resultValue = resultValue;
	}

	public RestResult(boolean success, Object resultValue, int status) {
		super();
		this.success = success;
		this.resultValue = resultValue;
		this.status = status;
	}

	@Override
	public String toString() {
		return "RestResult [success=" + success + ", resultValue=" +
                resultValue + ", status=" + status + "]";
	}

}
