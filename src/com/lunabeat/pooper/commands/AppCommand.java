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
package com.lunabeat.pooper.commands;

import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.lunabeat.dooper.ClusterConfig;
import com.lunabeat.dooper.ClusterInstance;
import com.lunabeat.dooper.ClusterList;
import com.lunabeat.dooper.CmdException;
import com.lunabeat.dooper.CmdSessionResult;
import com.lunabeat.dooper.HadoopCluster;
import com.lunabeat.dooper.MasterTimeoutException;
import com.lunabeat.dooper.SCPException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static java.lang.System.out;

/**
 *
 * @author cory
 */
public class AppCommand {

	private static final Log LOG = LogFactory.getLog(AppCommand.class.getName());
	private static final Map<String, CommandInfo> COMMANDS = initCommands();
	private ClusterConfig _config;

	public AppCommand(ClusterConfig config) {
		_config = config;
	}

	private static Map<String, CommandInfo> initCommands() {
		Map<String, CommandInfo> tmpMap = new HashMap<String, CommandInfo>();
		tmpMap.put("list-clusters", new CommandInfo(0, null, "List all ec2-hadoop cluster names."));
		tmpMap.put("delete-cluster", new CommandInfo(1, new String[]{"name:string"}, "Delete groups for ec2-cluster."));
		tmpMap.put("launch-cluster", new CommandInfo(2, new String[]{"name:string", "size:instanceSize", "nodes:int"}, "Launch a new Hadoop cluster."));
		tmpMap.put("launch-master", new CommandInfo(3, new String[]{"name:string", "size:instanceSize"}, "Launch a new Hadoop master."));
		tmpMap.put("launch-slaves", new CommandInfo(4, new String[]{"name:string", "size:instanceSize", "nodes:int"}, "Launch slaves for existing cluster."));
		tmpMap.put("terminate-cluster", new CommandInfo(5, new String[]{"name:string"}, "Terminate all instances in a  Hadoop cluster."));
		tmpMap.put("terminate-slaves", new CommandInfo(6, new String[]{"name:string", "nodes:int"}, "Terminate slaves in a cluster."));
		tmpMap.put("describe-cluster", new CommandInfo(7, new String[]{"name:string"}, "Get instance info for a cluster."));
		tmpMap.put("push-file", new CommandInfo(8, new String[]{"clusterName:string", "instances:instances", "srcPath:string", "destPath:string"}, "Copy file to cluster machines."));
		tmpMap.put("create-groups", new CommandInfo(9, new String[]{"clusterName:string"}, "Create security groups for cluster"));
		tmpMap.put("get-login-command", new CommandInfo(10, new String[]{"target:string"}, "Get remote login command for master or specified instance id. This command is for external scripts."));
		tmpMap.put("login", new CommandInfo(11, new String[]{"target:string"}, "Launch remote login for master or specific instance."));
		tmpMap.put("command", new CommandInfo(12, new String[]{"clusterName:string", "instances:instances", "command:string"}, "Run command on cluster machines."));

		return Collections.unmodifiableMap(tmpMap);
	}

	public static Set<String> commandNames() {
		return COMMANDS.keySet();
	}

	public static CommandInfo commandInfo(String name) {
		return COMMANDS.get(name);
	}

	public void runAppCommand(String commandName, String[] args) {

		CommandInfo cInfo = COMMANDS.get(commandName);
		if (cInfo == null) {
			out.println("INVALID COMMAND: '" + commandName + "'.");
			System.exit(1);
		}
		try {
			checkCommandArgs(commandName, cInfo, args);
		} catch (ArgumentException e) {
			out.println(e.getProblem());
			System.exit(1);
		}
		out.println("Running " + commandName);
		switch (cInfo.getIndex()) {
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
			case 7:
				describeCluster(args[0]);
				break;
			case 8:
				pushFile(args);
				break;
			case 9:
				createGroups(args[0]);
				break;
			case 10:
				getLoginCommand(args[0]);
				break;
			case 11:
				LOG.fatal("Direct call to login.");
				out.println("COMMAND 'login' needs to be handled by external script for now.\nUse 'get-login-command' to retrieve command and exec in shell.");
				break;
			case 12:
				runRemoteCommand(args);
				break;
			default:
				throw new RuntimeException("Bad AppInfo index for '" + commandName + "' :" + cInfo.getIndex());
		}
	}

	private void checkCommandArgs(String commandName, CommandInfo cInfo, String[] args) throws ArgumentException {

		String[] requiredArgNames = cInfo.getRequiredArgNames();
		String[] requiredArgTypes = cInfo.getRequiredArgTypes();

		if (requiredArgNames == null) {
			return;
		}
		if (args.length < requiredArgNames.length) {
			StringBuilder sb = new StringBuilder("Command '");
			sb.append(commandName).append("' requires ").
					append(requiredArgNames.length).
					append(" arguments ").
					append(args.length).
					append(" found.\nargs:");
			for (int x = 0; x < requiredArgNames.length; x++) {
				sb.append(" ").
						append(requiredArgNames[x]).
						append("(").
						append(requiredArgTypes[x]).
						append(")");
			}
			throw new ArgumentException(sb.toString());
		}

		for (int x = 0; x < requiredArgNames.length; x++) {
			String argName = requiredArgNames[x];
			String type = requiredArgTypes[x];
			if ("string".contentEquals(type)) {
				continue;
			}
			if ("int".contentEquals(type)) {
				try {
					Integer.parseInt(args[x]);
				} catch (NumberFormatException e) {
					throw new ArgumentException("Command '" + commandName + "' requires integer value for " + argName + ".");
				}
			} else if ("instanceSize".contentEquals(type)) {
				if (!ClusterConfig.INSTANCE_TYPES.contains(args[x])) {
					StringBuilder sb = new StringBuilder("Invalid instance size '").append(args[x]).append("'.\n").append("\tvalid sizes are:\n");
					for (String s : ClusterConfig.INSTANCE_TYPES) {
						sb.append("\t\t").append(s).append("\n");
					}
					throw new ArgumentException("Command '" + commandName + "'\n" + sb.toString());
				}
			} else if ("instances".contentEquals(type)) {

				if (!ClusterConfig.INSTANCE_GROUP_TYPES.contains(args[x])) {
					StringBuilder sb =
							new StringBuilder("Invalid instance '").append(args[x]).
							append("'.\n").append("\tvalid instances are:\n");
					for (String s : ClusterConfig.INSTANCE_GROUP_TYPES) {
						sb.append("\t\t").append(s).append("\n");
					}
					throw new ArgumentException("Command '" + commandName + "'\n" + sb.toString());
				}
			}


		}
	}

	private void listClusters() {
		Map<String, Map<String, List<Instance>>> outMap = ClusterList.getClusterMap(_config);
		out.println("found running clusters:");
		HashSet<String> emptyClusters = new HashSet<String>();
		for (String clustername : outMap.keySet()) {
			//out.println(clustername);
			boolean empty = true;
			for (String group : outMap.get(clustername).keySet()) {
				if (outMap.get(clustername).get(group).size() < 1) {
					emptyClusters.add(group.replace(HadoopCluster.MASTER_SUFFIX, ""));
				} else {
					out.println(group + " (" + outMap.get(clustername).get(group).size() + ")");
					empty = false;
				}
				for (Instance i : outMap.get(clustername).get(group)) {
					StringBuilder sb = new StringBuilder("\t");
					sb.append(i.getInstanceId());
					sb.append("\t");
					sb.append(i.getInstanceType());
					sb.append("\t");
					sb.append(i.getState().getName());
					//sb.append("\t");
					//sb.append(i.getPublicDnsName());
					out.println(sb.toString());
				}
				if (!empty) {
					out.println();
				}
			}
			if (!empty) {
				out.println("-----------");
			}
		}
		out.println("Found empty cluster security groups:");

		for (String s : emptyClusters) {
			out.println("\t" + s);
		}
	}

	private void deleteCluster(String clusterName) {
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		if (!cluster.groupsExist()) {
			out.println("Cluster '" + clusterName + "' does not exist.");
			return;
		}

		if (cluster.removeSecurityGroups()) {
			out.println("Deleted cluster '" + clusterName + "'.");
		} else {
			out.println("'" + clusterName + "' has instances and was not deleted.");
		}

	}

	private void launchCluster(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		int nodes = Integer.parseInt(args[2]);
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		try {
			out.println("launching master.");
			RunInstancesResult mr = cluster.launchMaster(instanceSize);
			if (mr == null) {
				out.println("Launch master failed!");
				System.exit(100);
			}
			Reservation r = mr.getReservation();
			Instance i = r.getInstances().get(0);
			if (i == null) {
				out.println("Launch master failed! reservation id: " + r.getReservationId());
				System.exit(100);
			}
			out.println("reservation id: " + r.getReservationId());
			out.println("\tinstance: " + i.getInstanceId() + "\t" + i.getState().getName());

			out.println("launching slaves (" + nodes + ").");
			out.println("waiting for master to get address.");
			RunInstancesResult sr = cluster.launchSlaves(nodes, instanceSize);
			if (sr == null) {
				out.println("Launch slaves failed!");
				System.exit(100);
			}
			Reservation sres = sr.getReservation();
			List<Instance> sis = sres.getInstances();
			if (sis == null || sis.isEmpty()) {
				out.println("Launch slaves failed! reservation id: " + sres.getReservationId());
				System.exit(100);
			}
			out.println("reservation id: " + sres.getReservationId());
			for (Instance si : sis) {
				out.println("\tinstance: " + si.getInstanceId() + "\t" + si.getState().getName());
			}
			out.println("Success.");
		} catch (IOException e) {
			out.println("IOException during userdata file encoding.");
			out.println(e.getMessage());
			System.exit(100);
		} catch (MasterTimeoutException e) {
			out.println("Timed out waiting for master to start.\nDon't panic.\nTry: 'pooper launch-slaves "
					+ e.group() + " " + e.size() + " " + e.howMany() + "' after master is running.\n" + "master id: " + e.masterId());
			System.exit(0);
		}
	}

	private void launchMaster(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		try {
			out.println("launching master.");
			RunInstancesResult mr = cluster.launchMaster(instanceSize);
			if (mr == null) {
				out.println("Launch master failed!");
				System.exit(100);
			}
			Reservation r = mr.getReservation();
			Instance i = r.getInstances().get(0);
			if (i == null) {
				out.println("Launch master failed! reservation id: " + r.getReservationId());
				System.exit(100);
			}
			out.println("reservation id: " + r.getReservationId());
			out.println("\tinstance: " + i.getInstanceId() + "\t" + i.getState().getName());
			out.println("Success.");
		} catch (IOException e) {
			out.println("IOException during userdata file encoding.");
			out.println(e.getMessage());
			System.exit(100);
		}
	}

	private void launchSlaves(String[] args) {
		String clusterName = args[0];
		String instanceSize = args[1];
		int nodes = Integer.parseInt(args[2]);
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		try {
			out.println("launching slaves (" + nodes + ").");
			RunInstancesResult sr = cluster.launchSlaves(nodes, instanceSize);
			if (sr == null) {
				out.println("Launch slaves failed!");
				System.exit(100);
			}
			Reservation sres = sr.getReservation();
			List<Instance> sis = sres.getInstances();
			if (sis == null || sis.isEmpty()) {
				out.println("Launch slaves failed! reservation id: " + sres.getReservationId());
				System.exit(100);
			}
			out.println("reservation id: " + sres.getReservationId());
			for (Instance si : sis) {
				out.println("\tinstance: " + si.getInstanceId() + "\t" + si.getState().getName());
			}
			out.println("Success.");
		} catch (IOException e) {
			out.println("IOException during userdata file encoding.");
			out.println(e.getMessage());
			System.exit(100);
		} catch (MasterTimeoutException e) {
			out.println("Timed out waiting for master to start.\nDon't panic.\nTry: 'pooper launch-slaves "
					+ e.group() + " " + e.size() + " " + e.howMany() + "' after master is running.\n" + "master id: " + e.masterId());
			System.exit(0);
		}
	}

	private void terminateCluster(String clusterName) {
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		TerminateInstancesResult tr = cluster.terminateCluster();
		if (tr == null || tr.getTerminatingInstances().isEmpty()) {
			out.println("no instances terminated.");
			System.exit(0);
		}
		out.println("Terminating " + tr.getTerminatingInstances().size() + " instances.");
		for (InstanceStateChange i : tr.getTerminatingInstances()) {
			out.println("\t" + i.getInstanceId() + " "
					+ i.getPreviousState().getName()
					+ " -> " + i.getCurrentState().getName());
		}
		out.println("Success.");
	}

	private void terminateSlaves(String[] args) {
		String clusterName = args[0];
		int nodes = Integer.parseInt(args[1]);
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		TerminateInstancesResult tr = cluster.terminateSlaves(nodes);
		if (tr == null || tr.getTerminatingInstances().isEmpty()) {
			out.println("no instances terminated.");
			System.exit(0);
		}
		out.println("Terminating " + tr.getTerminatingInstances().size() + " instances.");
		for (InstanceStateChange i : tr.getTerminatingInstances()) {
			out.println("\t" + i.getInstanceId() + " "
					+ i.getPreviousState().getName()
					+ "-> " + i.getCurrentState().getName());
		}
		out.println("Success.");
	}

	private void describeCluster(String name) {
		HadoopCluster cluster = new HadoopCluster(name, _config);
		if (!cluster.groupsExist()) {
			out.println("Cluster " + name + " not found.");
			System.exit(0);
		}
		out.println(name);
		out.println("---------------");
		out.println("Master:");
		out.println("-------");
		ClusterInstance master = cluster.getMaster();
		if (master == null) {
			out.println("no master found.");
		} else {
			out.println("id:\t\t" + master.getInstance().getInstanceId());
			out.println("state:\t\t" + master.getInstance().getState().getName());
			out.println("size:\t\t" + master.getInstance().getInstanceType());
			out.println("ami:\t\t" + master.getInstance().getImageId());
			out.println("public:\t\t" + master.getInstance().getPublicDnsName());
			out.println("privte:\t\t" + master.getInstance().getPrivateDnsName());
			out.println("launch:\t\t" + master.getInstance().getLaunchTime().toString());
			out.println("reserv:\t\t" + master.getReservationId());
			out.println("groups:\t\t" + master.getSecurityGroups());
		}
		out.println();
		out.println("Slaves:");
		out.println("-------");
		List<ClusterInstance> slaves = cluster.getSlaves();
		if (master == null) {
			out.println("no slaves found.");
		} else {
			for (ClusterInstance slave : slaves) {
				out.println("id:\t\t" + slave.getInstance().getInstanceId());
				out.println("state:\t\t" + slave.getInstance().getState().getName());
				out.println("size:\t\t" + slave.getInstance().getInstanceType());
				out.println("ami id:\t\t" + slave.getInstance().getImageId());
				out.println("public:\t\t" + slave.getInstance().getPublicDnsName());
				out.println("privat:\t\t" + slave.getInstance().getPrivateDnsName());
				out.println("launch:\t\t" + slave.getInstance().getLaunchTime().toString());
				out.println("reserv:\t\t" + slave.getReservationId());
				out.println("groups:\t\t" + slave.getSecurityGroups());
				out.println("---");
			}
		}

	}

	private void pushFile(String[] args) {
		//clusterName:string","instances:instances","srcPath:string","destPath:string"
		String clusterName = args[0];
		String target = args[1];
		String src = args[2];
		String dest = args[3];
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		try {
			if ("master".contentEquals(target) || "cluster".contentEquals(target)) {
				out.println("Pushing to master.");
				if (cluster.getMaster() == null) {
					out.println("No master found.");
				}
				cluster.putFile(cluster.getMaster(), src, dest);
				out.println("Copied to " + cluster.getMaster().getInstance().getInstanceId() + ".");
			}
			if ("slaves".contentEquals(target) || "cluster".contentEquals(target)) {
				out.println("Pushing to slaves.");
				List<ClusterInstance> slaves = cluster.getSlaves();
				if (slaves == null) {
					out.println("No slaves found.");
				}
				out.println("Copying to " + slaves.size() + " slaves.");
				for (ClusterInstance slave : slaves) {
					cluster.putFile(slave, src, dest);
					out.println("Copied to " + slave.getInstance().getInstanceId() + ".");
				}
			}

			out.println("Copied " + src + " to " + dest + " on " + target + ".");
		} catch (SCPException scpe) {
			boolean isMaster = scpe.getInstance().getInstance().getInstanceId().contentEquals(cluster.getMaster().getInstance().getInstanceId());
			out.println("Error pushing to " + (isMaster ? "master" : "slave"));
			out.println("InstanceId: " + scpe.getInstance().getInstance().getInstanceId());
			out.println("message: " + scpe.getMessage());
			if (scpe.getCause() != null) {
				out.println("cause: " + scpe.getCause().getMessage());
			}
			System.exit(1);
		}

	}

	private void createGroups(String clusterName) {
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		cluster.createSecurityGroups();
		out.println("Created groups for " + clusterName + ".");
	}

	private void getLoginCommand(String target) {
		String host = null;
		HadoopCluster cluster = new HadoopCluster(target, _config);
		if(cluster.groupsExist()){
			if(cluster.getMaster() != null)
				host = cluster.getMaster().getInstance().getPublicDnsName();
		}else if(target.startsWith(ClusterConfig.EC2_INSTANCE_PREFIX)){
			DescribeInstancesResult ir = cluster.getInstanceForId(target);
			LOG.info(ir.toString());
			if(ir.getReservations().size() > 0){
				if(ir.getReservations().get(0).getInstances().size() > 0){
					host = ir.getReservations().get(0).getInstances().get(0).getPublicDnsName();
				}
			}
		}
		if(host == null){
			out.println("echo  error: '" + target + "' is not a valid cluster name or instance id.");
			System.exit(0);
		}
		StringBuilder sb = new StringBuilder("ssh -i")
				.append(_config.get(ClusterConfig.KEYPAIR_FILE_KEY))
				.append(" ")
				.append(_config.get(ClusterConfig.USERNAME_KEY))
				.append("@")
				.append(host);
		out.println(sb.toString());
	}

	private void runRemoteCommand(String[] args){
		String clusterName = args[0];
		String target = args[1];
		StringBuilder sb = new StringBuilder(args[2]);
		for(int x =3; x < args.length;x++){
			sb.append(" ").append(args[x]);
		}
		String command = sb.toString();
		HadoopCluster cluster = new HadoopCluster(clusterName, _config);
		ArrayList<CmdSessionResult> results = new ArrayList<CmdSessionResult>();
		try {

			if ("master".contentEquals(target) || "cluster".contentEquals(target)) {
				out.println("Running command on master.");
				if (cluster.getMaster() == null) {
					out.println("No master found.");
				}
				results.add(cluster.remoteCommand(cluster.getMaster(), command));
			}
			if ("slaves".contentEquals(target) || "cluster".contentEquals(target)) {
				out.println("Running command on slaves.");
				List<ClusterInstance> slaves = cluster.getSlaves();
				if (slaves == null) {
					out.println("No slaves found.");
				}
				out.println("Running on " + slaves.size() + " slaves.");
				results.addAll(cluster.remoteCommand(slaves, command));
				
			}

			out.println("Successfully Ran '" + command + "' on " + target + ".");
			out.println("Report:");
			for(CmdSessionResult result:results){
				out.println("----------");
				out.println("Instance: " + result.getInstance().getInstance().getInstanceId());
				out.println("Exit Status: " + result.getCode());
				if(result.getStdout().length() > 0){
					out.println("STDOUT:");
					out.println(result.getStdout());
				}
				if(result.getStderr().length() > 0){
					out.println("STDERR:");
					out.println(result.getStderr());
				}
				out.println("----------");
			}
		} catch (CmdException cmde) {
			boolean isMaster = cmde.getInstance().getInstance().getInstanceId().contentEquals(cluster.getMaster().getInstance().getInstanceId());
			out.println("Error running command on " + (isMaster ? "master" : "slave"));
			out.println("InstanceId: " + cmde.getInstance().getInstance().getInstanceId());
			out.println("message: " + cmde.getMessage());
			if (cmde.getCause() != null) {
				out.println("cause: " + cmde.getCause().getMessage());
			}
			System.exit(1);
		}
	}

	public static class CommandInfo {

		private String[] _requiredArgNames;
		private String[] _requiredArgTypes;
		private String _description;
		private int _index = -1;

		/**
		 *
		 * @param requiredArgs
		 * @param description
		 */
		public CommandInfo(int index, String[] requiredArgs, String description) {
			if (requiredArgs == null) {
				_requiredArgNames = null;
				_requiredArgTypes = null;
			} else {
				_requiredArgNames = new String[requiredArgs.length];
				_requiredArgTypes = new String[requiredArgs.length];
				for (int x = 0; x < requiredArgs.length; x++) {
					String[] parts = requiredArgs[x].split(":");
					if (parts.length != 2) {
						throw new RuntimeException("Bad required arg given to CommandInfo - '" + requiredArgs[x] + "'.");
					}
					_requiredArgNames[x] = parts[0];
					_requiredArgTypes[x] = parts[1];
				}
			}

			_description = description;
			_index = index;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return _description;
		}

		/**
		 * @return the index
		 */
		public int getIndex() {
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
