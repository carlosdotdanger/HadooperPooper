/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.dooper;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cory
 */
public class ClusterList {

	public static Map<String, Map<String, Integer>> listClusterGroups(ClusterConfig config) {
		HashMap<String, Map<String, Integer>> groupMap = new HashMap<String, Map<String, Integer>>();

		return groupMap;
	}

	private ClusterList(){};
	private ClusterList(ClusterList c){};

	public static Map<String,Map<String,List<Instance>>> getClusterMap(ClusterConfig config){
		HashMap<String,Map<String,List<Instance>>> clusterMap  = new HashMap<String,Map<String,List<Instance>>>();
		AmazonEC2Client ec2 = new AmazonEC2Client(config);
		DescribeSecurityGroupsRequest dsr = new DescribeSecurityGroupsRequest().withFilters(new Filter().withName(HadoopCluster.GROUP_NAME_KEY).withValues("*"+HadoopCluster.MASTER_SUFFIX));
		DescribeSecurityGroupsResult groupsResult = ec2.describeSecurityGroups(dsr);
		if(groupsResult == null)
			return null;
		List<SecurityGroup> groups = groupsResult.getSecurityGroups();
		ArrayList<String> groupNames = new ArrayList<String>();
		for(SecurityGroup sg:groups){
			groupNames.add(sg.getGroupName());
			groupNames.add(sg.getGroupName().replace(HadoopCluster.MASTER_SUFFIX,""));
		}
		for(String group:groupNames){
			String clusterName = group.replace(HadoopCluster.MASTER_SUFFIX,"");
			if (clusterMap.get(clusterName) == null)
				clusterMap.put(clusterName, new HashMap<String,List<Instance>>());
			if(clusterMap.get(clusterName).get(group) == null)
				clusterMap.get(clusterName).put(group, new ArrayList<Instance>());
		}
		DescribeInstancesRequest dir = new DescribeInstancesRequest().withFilters(new Filter().withName(HadoopCluster.GROUP_NAME_KEY).withValues(groupNames));
		DescribeInstancesResult instanceResult = ec2.describeInstances(dir);
		if(instanceResult == null)
			return null;
		for(Reservation r:instanceResult.getReservations()){
			String group = r.getGroupNames().get(0);
			String clusterName = group.replace(HadoopCluster.MASTER_SUFFIX,"");
			if (clusterMap.get(clusterName) == null)
				clusterMap.put(clusterName, new HashMap<String,List<Instance>>());
			if(clusterMap.get(clusterName).get(group) == null)
				clusterMap.get(clusterName).put(group, new ArrayList<Instance>());		
			for(Instance i:r.getInstances()){
				clusterMap.get(clusterName).get(group).add(i);
			}
		}

		return clusterMap;
	}

}
