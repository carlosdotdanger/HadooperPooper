/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.dooper;

/**
 *
 * @author cory
 */
public class MasterTimeoutException extends Exception {
	private String _group;
	private int _howMany;
	private String _masterId;
	private String _size;

	/**
	 * 
	 * @param group
	 * @param howMany
	 * @param size
	 * @param masterId
	 */
	public MasterTimeoutException(String group,int howMany,String size,String masterId){
		_group = group;
		_howMany = howMany;
		_size  = size;
		_masterId = masterId;

	}


	/**
	 * @return group
	 */ public String group() {
		return _group;
	}

	/**
	 * @return howMany
	 */ public int howMany() {
		return _howMany;
	}

	/**
	 * @return masterId
	 */ public String masterId() {
		return _masterId;
	}

	/**
	 * @return size
	 */ public String size() {
		return _size;
	}



}
