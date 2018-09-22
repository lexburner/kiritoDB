package com.alibabacloud.polar_race.engine.common.exceptions;
/**
 * When egine has some Exception throw,you can use this EngineException.
 * RetCodeEnum define some common return code and error code.
 *
 */
public class EngineException extends Exception {
	public RetCodeEnum retCode;
	
	public EngineException(RetCodeEnum retCode, String msg) {
		super(msg);
		this.retCode = retCode;
	}
	

}
