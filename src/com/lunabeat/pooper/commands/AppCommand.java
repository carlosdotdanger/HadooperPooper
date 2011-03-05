/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.pooper.commands;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.lunabeat.dooper.ClusterConfig;
import com.lunabeat.dooper.ClusterList;
import com.lunabeat.dooper.HadoopCluster;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author cory
 */
public class AppCommand {
	private static final Map<String, CommandInfo> COMMANDS = initCommands();
	private ClusterConfig _config;

	public AppCommand(ClusterConfig config){
		_config = config;
	}

	private static Map<String, CommandInfo> initCommands() {
		Map<String, CommandInfo> tmpMap = new HashMap<String, CommandInfo>();
		tmpMap.put("list-clusters", new CommandInfo(0,null,"List all ec2-hadoop cluster names."));
		tmpMap.put("delete-cluster", new CommandInfo(1,new String[]{"name:string"},"Terminate all instances and delete groups for ec2-cluster."));
		tmpMap.put("launch-cluster", new CommandInfo(2,new String[]{"name:string","size:instanceSize","nodes:int"},"Launch a new Hadoop cluster."));
		tmpMap.put("launch-master", new CommandInfo(3,new String[]{"name:string","size:instanceSize"},"Launch a new Hadoop master."));
		tmpMap.put("launch-slaves", new CommandInfo(4,new String[]{"name:string","size:instanceSize","nodes:int"},"Launch slaves for existing cluster."));
		tmpMap.put("terminate-cluster", new CommandInfo(5,new String[]{"name:string"},"Terminate all instances in a  Hadoop cluster."));
		tmpMap.put("terminate-slaves", new CommandInfo(6,new String[]{"name:string","nodes:int"},"Terminate slaves in a cluster."));
		return Collections.unmodifiableMap(tmpMap);
	}

	public static Set<String> commandNames(){
		return COMMANDS.keySet();
	}

	public static CommandInfo commandInfo(String name){
		return COMMANDS.get(name);
	}

	public void runCommand(String commandName,String[] args){
		
		CommandInfo cInfo = COMMANDS.get(commandName);
		if(cInfo == null){
			System.out.println("INVALID COMMAND: '" + commandName + "'.");
			System.exit(1);
		}
		try{
			checkCommandArgs(commandName,cInfo,args);
		}catch (ArgumentException e) {
			System.out.println(e.getProblem());
			System.exit(1);
		}
		System.out.println("Running "+ commandName);
		switch(cInfo.getIndex()){
			case 0:
				listClusters();
				break;
			case 1:
				deleteCluster(args[0]);
				break;
			case 2:
				launchCluster(args);
				break;
			case 3:
				launchMaster(args);
				break;
			case 4:
				launchSlaves(args);
				break;
			case 5:
				terminateCluster(args[0]);
				break;
			case 6:
				terminateSlaves(args);
				break;
			default:
				throw new RuntimeException("Bad AppInfo index for '" +commandName + "' :" + cInfo.getIndex());
		}
	}

	private void checkCommandArgs(String commandName ,CommandInfo cInfo, String[] args) throws ArgumentException {

		String[] requiredArgNames = cInfo.getRequiredArgNames();
		String[] requiredArgTypes = cInfo.getRequiredArgTypes();

		if(requiredArgNames == null)
			return;
		if(args.length != requiredArgNames.length){
			StringBuilder sb = new StringBuilder("Command '");
			sb.append(commandName).append("' requires ").
					append(requiredArgNames.length).
					append(" arguments ").
					append(args.length).
					append(" found.\nargs:");
					for(int x=0;x<requiredArgNames.length;x++){
						sb.append(" ").
						append(requiredArgNames[x]).
						append("(").
						append(requiredArgTypes[x]).
						append(")");
					}
			throw new ArgumentException(sb.toString());
		}
		
		for( int x=0; x < requiredArgNames.length;x++){
			String argName = requiredArgNames[x];
			String type = requiredArgTypes[x];
			if("string".contentEquals(type)){
				continue;
			}
			if("int".contentEquals(type)){
				try{
					Integer.parseInt(args[x]);
				}catch(NumberFormatException e){
					throw new ArgumentException("Command '" + commandName + "' requires integer value for " + argName + ".");
				}
			}else if("instanceSize".contentEquals(type)){
				if(!ClusterConfig.INSTANCE_TYPES.contains(args[x])){
					StringBuilder sb = new StringBuilder("Invalid instance size '");
					sb.append(args[x]).append("'.\n").append("\tvalid sizes are:\n");
					for(String s:ClusterConfig.INSTANCE_TYPES){
						sb.append("\t\t").append(s).append("\n");
					}
					throw new ArgumentException("Command '" + commandName + "'\n" + sb.toString());
				}
			}


		}
	}

	private void listClusters() {
		Map<String,Map<String,List<Instance>>> out = ClusterList.getClusterMap(_config);
		System.out.println("found clusters:");
		for(String clustername:out.keySet()){
			System.out.println(clustername);
			for(String group: out.get(clustername).keySet()){
				if(out.get(clustername).get(group).size() < 1)
					System.out.println("\t"+group+" (empty)");
				else
					System.out.println("\t"+group);
				for(Instance i: out.get(clustername).get(group)){
					StringBuilder sb = new StringBuilder("\t");
					sb.append(i.getInstanceId());
					sb.append("\t");
					sb.append(i.getPublicDnsName());
					sb.append("\t");
					sb.append(i.getState().getName());
					sb.append("\t");
					sb.append(i.getInstanceType());
					sb.append("\t");
					sb.append(i.getLaunchTime().toString());
					System.out.println(sb.toString());
				}
			}
		}
	}

	private void deleteCluster(String clusterName){
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		if(!cluster.groupsExist()){
			System.out.println("Cluster '" + clusterName + "' does not exist.");
			return;
		}
		TerminateInstancesResult mres = cluster.terminateMaster();
		TerminateInstancesResult sres = cluster.terminateAllSlaves();
		int termCount = 0;
		if(mres!=null)
			termCount += mres.getTerminatingInstances().size();
		if(sres!=null)
			termCount += sres.getTerminatingInstances().size();
		cluster.removeSecurityGroups();
		System.out.println("Deleted cluster '"+clusterName+"'.");
		if(termCount > 0 )
			System.out.println("Terminated "+termCount+" instances.");
	}


	private void listClusterGroups(){
		Map<String,Map<String,Integer>> out = ClusterList.listClusterGroups(_config);
		System.out.println("found clusters:");

	}

	private void launchCluster(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		int nodes = Integer.parseInt(args[2]);
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		try{
			System.out.println("launching master.");
			RunInstancesResult mr = cluster.launchMaster(instanceSize);
			if(mr == null){
				System.out.println("Launch master failed!");
				System.exit(100);
			}
			Reservation r  = mr.getReservation();
			Instance i = r.getInstances().get(0);
			if(i == null){
				System.out.println("Launch master failed! reservation id: " + r.getReservationId());
				System.exit(100);
			}
			System.out.println("reservation id: "+ r.getReservationId());
			System.out.println("\tinstance: " + i.getInstanceId()+"\t"+i.getState().getName());
			
			System.out.println("launching slaves (" + nodes + ")." );
			System.out.println("waiting for master to get address." );
			RunInstancesResult sr = cluster.launchSlaves(nodes,instanceSize);
			if(sr == null){
				System.out.println("Launch slaves failed!");
				System.exit(100);
			}
			Reservation sres  = sr.getReservation();
			List<Instance> sis = sres.getInstances();
			if(sis == null || sis.isEmpty()){
				System.out.println("Launch slaves failed! reservation id: "+ sres.getReservationId());
				System.exit(100);
			}
			System.out.println("reservation id: "+ sres.getReservationId());
			for(Instance si:sis){
				System.out.println("\tinstance: " + si.getInstanceId()+"\t"+si.getState().getName());
			}
			System.out.println("Success.");
		}catch(IOException e){
			System.out.println("IOException during userdata file encoding.");
			System.out.println(e.getMessage());
			System.exit(100);
		}
	}



	private void launchMaster(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		try{
			System.out.println("launching master.");
			RunInstancesResult mr = cluster.launchMaster(instanceSize);
			if(mr == null){
				System.out.println("Launch master failed!");
				System.exit(100);
			}
			Reservation r  = mr.getReservation();
			Instance i = r.getInstances().get(0);
			if(i == null){
				System.out.println("Launch master failed! reservation id: " + r.getReservationId());
				System.exit(100);
			}
			System.out.println("reservation id: "+ r.getReservationId());
			System.out.println("\tinstance: " + i.getInstanceId()+"\t"+i.getState().getName());
			System.out.println("Success.");
		}catch(IOException e){
			System.out.println("IOException during userdata file encoding.");
			System.out.println(e.getMessage());
			System.exit(100);
		}
	}


	private void launchSlaves(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		int nodes = Integer.parseInt(args[2]);
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		try{
			System.out.println("launching slaves (" + nodes + ")." );
			RunInstancesResult sr = cluster.launchSlaves(nodes,instanceSize);
			if(sr == null){
				System.out.println("Launch slaves failed!");
				System.exit(100);
			}
			Reservation sres  = sr.getReservation();
			List<Instance> sis = sres.getInstances();
			if(sis == null || sis.isEmpty()){
				System.out.println("Launch slaves failed! reservation id: "+ sres.getReservationId());
				System.exit(100);
			}
			System.out.println("reservation id: "+ sres.getReservationId());
			for(Instance si:sis){
				System.out.println("\tinstance: " + si.getInstanceId()+"\t"+si.getState().getName());
			}
			System.out.println("Success.");
		}catch(IOException e){
			System.out.println("IOException during userdata file encoding.");
			System.out.println(e.getMessage());
			System.exit(100);
		}
	}

	private void terminateCluster(String clusterName){
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		TerminateInstancesResult tr = cluster.terminateCluster();
		if(tr == null || tr.getTerminatingInstances().isEmpty()){
			System.out.println("no instances terminated.");
			System.exit(0);
		}
		System.out.println("Terminating "+ tr.getTerminatingInstances().size() + " instances.");
		for(InstanceStateChange i : tr.getTerminatingInstances()){
			System.out.println("\t"+i.getInstanceId() + " " +
					i.getPreviousState().getName() +
					"-> " + i.getCurrentState().getName());
		}
		System.out.println("Success.");
	}

	private void terminateSlaves(String[] args){
		String clusterName = args[0];
		int nodes = Integer.parseInt(args[1]);
		HadoopCluster cluster = new HadoopCluster(clusterName,_config);
		TerminateInstancesResult tr = cluster.terminateSlaves(nodes);
		if(tr == null || tr.getTerminatingInstances().isEmpty()){
			System.out.println("no instances terminated.");
			System.exit(0);
		}
		System.out.println("Terminating "+ tr.getTerminatingInstances().size() + " instances.");
		for(InstanceStateChange i : tr.getTerminatingInstances()){
			System.out.println("\t"+i.getInstanceId() + " " +
					i.getPreviousState().getName() +
					"-> " + i.getCurrentState().getName());
		}
		System.out.println("Success.");
	}


	public static class CommandInfo{
		private String[] _requiredArgNames;
		private String[] _requiredArgTypes;
		private String _description;
		private int _index = -1;

		/**
		 *
		 * @param requiredArgs
		 * @param description
		 */
		public CommandInfo(int index,String[] requiredArgs,String description){
			if(requiredArgs == null){
				_requiredArgNames = null;
				_requiredArgTypes = null;
			}else{
			_requiredArgNames = new String[requiredArgs.length];
			_requiredArgTypes = new String[requiredArgs.length];
				for(int x = 0; x < requiredArgs.length;x++){
					String[] parts = requiredArgs[x].split(":");
					if(parts.length != 2)
						throw new RuntimeException("Bad required arg given to CommandInfo - '" + requiredArgs[x] + "'.");
					_requiredArgNames[x] = parts[0];
					_requiredArgTypes[x] = parts[1];
				}
			}

			_description = description;
			_index = index;
		}

		/**
		 * @return the description
		 */ public String getDescription() {
			return _description;
		}

		/**
		 * @return the index
		 */ public int getIndex() {
			return _index;
		}

		/**
		 *
		 * @return array of required arg names.
		 */
		private String[] getRequiredArgNames() {
			return _requiredArgNames;
		}
		
		/**
		 * 
		 * @return array of required arg types.
		 */
		private String[] getRequiredArgTypes() {
			return _requiredArgTypes;
		}

	}
}
