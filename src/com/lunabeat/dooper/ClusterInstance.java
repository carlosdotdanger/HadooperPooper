/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
