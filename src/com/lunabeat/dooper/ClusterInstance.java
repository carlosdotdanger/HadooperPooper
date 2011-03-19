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

import com.amazonaws.services.ec2.model.Instance;
import java.util.List;

/**
 *
 * @author cory
 */
public class ClusterInstance {
	private String _reservationId;
	private List<String> _secGroups;
	private Instance _instance;


	public ClusterInstance(Instance instance,String reservationId,List<String> securityGroups){
		_instance = instance;
		_reservationId = reservationId;
		_secGroups = securityGroups;
	}

	/**
	 * @return the _reservationId
	 */ public String getReservationId() {
		return _reservationId;
	}


	/**
	 * @return the _secGroups
	 */ public List<String> getSecurityGroups() {
		return _secGroups;
	}


	/**
	 * @return the _instance
	 */ public Instance getInstance() {
		return _instance;
	}
}
