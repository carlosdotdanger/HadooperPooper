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
