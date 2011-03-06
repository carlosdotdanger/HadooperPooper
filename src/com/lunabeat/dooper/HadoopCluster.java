/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lunabeat.dooper;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DeleteSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.model.UserIdGroupPair;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author cory
 */
public class HadoopCluster {

	public static final String MASTER_SUFFIX = "-master";
	public static final String GROUP_NAME_KEY = "group-name";
	private String _groupName = null;
	private String _masterGroupName = null;
	private ClusterInstance _master = null;
	private ArrayList<ClusterInstance> _slaves = new ArrayList<ClusterInstance>();
	private ClusterConfig _config = null;
	private AmazonEC2 _ec2 = null;
	private static final Integer LOW_PORT = 0;
	private static final Integer HI_PORT = 65535;
	private static final String ALL_IPS = "0.0.0.0/0";
	private static final String TCP = "tcp";
	private static final String UDP = "udp";
	private static final String ICMP = "icmp";
	private static final Pattern userDataValue = Pattern.compile("%([a-zA-Z0-9\\._-]+)%");
	private static final int WAIT_FOR_MASTER_MAX_TIMES = 5;
	private static final int WAIT_FOR_MASTER_INTERVAL_SECONDS = 30;

	/**
	 * 
	 * @param groupName
	 * @param credentials
	 */
	public HadoopCluster(String groupName, ClusterConfig credentials) {
		_groupName = groupName;
		_masterGroupName = _groupName.concat(MASTER_SUFFIX);
		_config = credentials;
	}

	private void init() {
		if (_ec2 == null) {
			_ec2 = new AmazonEC2Client(_config);
		}

	}

	private void update() {
		//unless force == true check the holdtime and bail if it's too soon.
		Date now = new Date();
		init();
		//get master + slave info
		_slaves.clear();
		_master = null;
		DescribeInstancesResult dir =
				_ec2.describeInstances(new DescribeInstancesRequest().withFilters(
				new Filter().withName(GROUP_NAME_KEY).withValues(getGroupName(), getMasterGroupName())));
		for (Reservation r : dir.getReservations()) {
			String rid = r.getReservationId();
			List<String> gnames = r.getGroupNames();
			for (Instance i : r.getInstances()) {
				if (gnames.contains(getMasterGroupName())) {
					_master = new ClusterInstance(i, rid, gnames);
				} else {
					_slaves.add(new ClusterInstance(i, rid, gnames));
				}
			}
		}
	}

	public List<ClusterInstance> getSlaves() {
		update();
		return _slaves;
	}

	public ClusterInstance getMaster() {
		update();
		return _master;
	}

	public RunInstancesResult launchMaster(String size) throws IOException {
		update();
		if ((_master != null) && ((InstanceStateName.Running == InstanceStateName.fromValue(_master.getInstance().getState().getName())) || (InstanceStateName.Pending == InstanceStateName.fromValue(_master.getInstance().getState().getName())))) {
			Reservation masterReservation =
					_ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(_master.getInstance().getInstanceId())).getReservations().get(0);
			return new RunInstancesResult().withReservation(masterReservation);
		}
		//make the groups
		createSecurityGroups();
		String AMIImage = (_config.get("AMI." + size + ".Image") == null)
				? _config.get(ClusterConfig.DEFAULT_AMI_KEY)
				: _config.get("AMI." + size + ".Image");
		System.out.println("AMIImage = [" + AMIImage + "]");
		RunInstancesRequest rir = new RunInstancesRequest().withImageId(AMIImage).
				withMinCount(1).
				withMaxCount(1).
				withInstanceType(size).
				withSecurityGroups(_masterGroupName).
				withUserData(Base64.encodeBase64String(getUserData().getBytes())).withKeyName(_config.get(ClusterConfig.KEYPAIR_NAME_KEY));
		return _ec2.runInstances(rir);
	}

	public RunInstancesResult launchSlaves(int howMany, String size) throws IOException {
		update();
		if (_master == null
				|| (InstanceStateName.Terminated
				== InstanceStateName.fromValue(_master.getInstance().getState().getName()))
				|| (InstanceStateName.ShuttingDown
				== InstanceStateName.fromValue(_master.getInstance().getState().getName()))) {
			return null;
		}
		//wait for master to get internal ip field to pass in userinfo
		boolean success = false;
		if (InstanceStateName.Pending == InstanceStateName.fromValue(_master.getInstance().getState().getName())) {
			int attempts = 0;
			while ((attempts < WAIT_FOR_MASTER_MAX_TIMES) && !success) {
				update();
				String pDns = _master.getInstance().getPrivateDnsName();
				if (pDns == null || pDns.length() < 6) {
					try {
						Thread.sleep(WAIT_FOR_MASTER_INTERVAL_SECONDS * 1000);
					} catch (InterruptedException ie) {
						return null;
					}
					attempts++;
				} else {
					success = true;
				}
			}
			if (!success) {
				System.out.println("Timed out waiting for master to start.\nDon't panic.\nTry: 'pooper launch-slaves "
						+ _groupName + " " + size + " " + howMany + "' after master is running.");
				System.exit(0);
			}
		}


		String AMIImage = (_config.get("AMI." + size + ".Image") == null)
				? _config.get(ClusterConfig.DEFAULT_AMI_KEY)
				: _config.get("AMI." + size + ".Image");
		RunInstancesRequest rir = new RunInstancesRequest().withImageId(AMIImage).
				withMinCount(howMany).
				withMaxCount(howMany).
				withInstanceType(size).
				withSecurityGroups(_groupName).
				withUserData(Base64.encodeBase64String(getUserData().getBytes())).
				withKeyName(_config.get(ClusterConfig.KEYPAIR_NAME_KEY));
		return _ec2.runInstances(rir);
	}

	public TerminateInstancesResult terminateCluster() {
		update();
		ArrayList<String> iids = new ArrayList<String>();
		if (_master != null) {
			iids.add(_master.getInstance().getInstanceId());
		}
		for (ClusterInstance ci : _slaves) {
			iids.add(ci.getInstance().getInstanceId());
		}
		if (iids.size() < 1) {
			return null;
		}
		TerminateInstancesRequest tir =
				new TerminateInstancesRequest().withInstanceIds(iids);
		return _ec2.terminateInstances(tir);
	}

	public TerminateInstancesResult terminateMaster() {
		update();

		if (_master == null) {
			return null;
		}
		TerminateInstancesRequest tir;
		tir = new TerminateInstancesRequest().withInstanceIds(_master.getInstance().getInstanceId());
		return _ec2.terminateInstances(tir);

	}

	public TerminateInstancesResult terminateAllSlaves() {
		update();
		return terminateSlaves(_slaves.size());
	}

	public TerminateInstancesResult terminateSlaves(int howMany) {
		update();
		int terminated = 0;
		ArrayList<String> iids = new ArrayList<String>();
		for (ClusterInstance slave : _slaves) {
			InstanceStateName state =
					InstanceStateName.fromValue(slave.getInstance().getState().getName());
			if (terminated < howMany && (state == InstanceStateName.Running || state == InstanceStateName.Pending)) {
				iids.add(slave.getInstance().getInstanceId());
				terminated++;
			}

		}
		if (iids.size() < 1) {
			return null;
		}
		TerminateInstancesRequest tir =
				new TerminateInstancesRequest().withInstanceIds(iids);
		return _ec2.terminateInstances(tir);
	}

	/**
	 * @return the _groupName
	 */
	public String getGroupName() {
		return _groupName;
	}

	/**
	 * @return the _masterGroupName
	 */
	public String getMasterGroupName() {
		return _masterGroupName;
	}

	public boolean groupsExist() {
		update();
		DescribeSecurityGroupsResult dsr =
				_ec2.describeSecurityGroups(new DescribeSecurityGroupsRequest().withFilters(new Filter(GROUP_NAME_KEY).withValues(_groupName, _masterGroupName)));
		if (dsr.getSecurityGroups().size() > 0) {
			return true;
		}
		return false;
	}

	public void createSecurityGroups() {
		if (groupsExist()) {
			return;
		}
		String portList = _config.get(ClusterConfig.WEB_PORTS_KEY);
		boolean hasWebPorts = false;
		List<Integer> webPorts = new ArrayList<Integer>();
		if (!"0".contentEquals(portList)) {
			String[] portParts = portList.split(",");
			for (String portString : portParts) {
				try {
					webPorts.add(Integer.parseInt(portString));
				} catch (NumberFormatException e) {
					throw new RuntimeException(ClusterConfig.WEB_PORTS_KEY + " config value must be list of ints or '0'");
				}
			}
		}
		UserIdGroupPair slaveUserIdGroupPair = new UserIdGroupPair().withGroupName(_groupName).withUserId(_config.get(ClusterConfig.ACCOUNT_ID_KEY));
		UserIdGroupPair masterUserIdGroupPair = new UserIdGroupPair().withGroupName(_masterGroupName).withUserId(_config.get(ClusterConfig.ACCOUNT_ID_KEY));
		CreateSecurityGroupRequest masterCsr = new CreateSecurityGroupRequest().withGroupName(_masterGroupName).
				withDescription("Master group created by hadooper-pooper.");
		_ec2.createSecurityGroup(masterCsr);
		CreateSecurityGroupRequest slaveCsr = new CreateSecurityGroupRequest().withGroupName(_groupName).
				withDescription("Slave group created by hadooper-pooper.");
		_ec2.createSecurityGroup(slaveCsr);
		ArrayList<IpPermission> ipPerms = new ArrayList<IpPermission>();
		ipPerms.add(new IpPermission().withToPort(22).withFromPort(22).withIpProtocol(TCP).withIpRanges(ALL_IPS));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(TCP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(UDP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(ICMP).withToPort(-1).withFromPort(-1));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(TCP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(UDP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(ICMP).withToPort(-1).withFromPort(-1));
		if (hasWebPorts) {
			for (int port : webPorts) {
				ipPerms.add(new IpPermission().withToPort(port).withFromPort(port).withIpProtocol(TCP).withIpRanges(ALL_IPS));
			}
		}


		AuthorizeSecurityGroupIngressRequest masterASR = new AuthorizeSecurityGroupIngressRequest().withGroupName(_masterGroupName).withIpPermissions(ipPerms);
		_ec2.authorizeSecurityGroupIngress(masterASR);

		AuthorizeSecurityGroupIngressRequest asr = new AuthorizeSecurityGroupIngressRequest().withGroupName(_groupName).withIpPermissions(ipPerms);
		_ec2.authorizeSecurityGroupIngress(asr);

	}

	public void removeSecurityGroups() {
		if (!groupsExist()) {
			return;
		}
		if (_master != null || _slaves.size() > 0) {
			return;
		}
		UserIdGroupPair slaveUserIdGroupPair = new UserIdGroupPair().withGroupName(_groupName).withUserId(_config.get(ClusterConfig.ACCOUNT_ID_KEY));
		UserIdGroupPair masterUserIdGroupPair = new UserIdGroupPair().withGroupName(_masterGroupName).withUserId(_config.get(ClusterConfig.ACCOUNT_ID_KEY));
		ArrayList<IpPermission> ipPerms = new ArrayList<IpPermission>();
		ipPerms.add(new IpPermission().withToPort(22).withFromPort(22).withIpProtocol(TCP).withIpRanges(ALL_IPS));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(TCP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(UDP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(masterUserIdGroupPair).withIpProtocol(ICMP).withToPort(-1).withFromPort(-1));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(TCP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(UDP).withToPort(HI_PORT).withFromPort(LOW_PORT));
		ipPerms.add(new IpPermission().withUserIdGroupPairs(slaveUserIdGroupPair).withIpProtocol(ICMP).withToPort(-1).withFromPort(-1));
		RevokeSecurityGroupIngressRequest srsgi = new RevokeSecurityGroupIngressRequest().withGroupName(_groupName).withIpPermissions(ipPerms);
		_ec2.revokeSecurityGroupIngress(srsgi);
		RevokeSecurityGroupIngressRequest mrsgi = new RevokeSecurityGroupIngressRequest().withGroupName(_masterGroupName).withIpPermissions(ipPerms);
		_ec2.revokeSecurityGroupIngress(mrsgi);
		_ec2.deleteSecurityGroup(new DeleteSecurityGroupRequest().withGroupName(_groupName));
		_ec2.deleteSecurityGroup(new DeleteSecurityGroupRequest().withGroupName(_masterGroupName));

	}

	public String getUserData() throws IOException {
		update();
		StringBuilder userData = new StringBuilder();
		FileReader fr = new FileReader(_config.get(ClusterConfig.USER_DATA_PATH_KEY));
		if (fr == null) {
			throw new IOException("Could NOT open resource: [" + ClusterConfig.USER_DATA_PATH_KEY + "]");
		}
		BufferedReader buf = new BufferedReader(fr);
		String line = null;
		while ((line = buf.readLine()) != null) {
			Matcher matches = userDataValue.matcher(line);
			if (!matches.find()) {
				userData.append(line);
				userData.append("\n");
				continue;
			}

			String key = matches.group(1);
			String value = "\"\"";
			if (key.contentEquals(ClusterConfig.MASTER_HOST_KEY)) {
				if (_master != null) {
					value = _master.getInstance().getPrivateDnsName();
				}
			} else {
				value = _config.get(key, value);
			}
			line = line.replaceAll("%" + key + "%", value);
			userData.append(line);
			userData.append("\n");
		}

		if (userData.length() < 1) {
			return null;
		}
		return userData.toString();
	}
}
