/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.dooper;

/**
 *
 * @author cory
 */
public class SCPException extends Exception {
	ClusterInstance _instance;

	/**
	 * 
	 * @param message
	 * @param host
	 */
	SCPException(String message,ClusterInstance host){
		super(message);
		_instance = host;
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 * @param host
	 */
	SCPException(String message,Throwable cause,ClusterInstance host){
		super(message,cause);
		_instance = host;
	}

	/**
	 * 
	 * @return
	 */
	public ClusterInstance getInstance(){
		return _instance;
	}
}
