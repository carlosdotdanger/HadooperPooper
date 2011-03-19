/***********************************************
*    Copyright [2011] [carlosdotdanger]
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
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
