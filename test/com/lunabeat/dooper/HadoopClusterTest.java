/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.lunabeat.dooper;

import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static java.lang.System.out;

/**
 *
 * @author cory
 */
public class HadoopClusterTest {

	private HadoopCluster cluster = null;
	private static final String TEST_GROUP = "testr";
	private static final String TEST_SIZE = "m1.large";
	private static ClusterConfig _config = null;

    public HadoopClusterTest() {
    }

	@BeforeClass
	public static void setUpClass() throws Exception {
		out.println("setting up!");
		_config = new ClusterConfig(HadoopClusterTest.class.getResourceAsStream("/com/lunabeat/TestConfig.properties"));

	}

	@AfterClass
	public static void tearDownClass() throws Exception {
	}

    @Before
    public void setUp() throws IOException {
		cluster = new HadoopCluster(TEST_GROUP,_config);
    }

    @After
    public void tearDown() {
    }

	/**
	 * Test for createSecurityGroups
	 */@Test
	public void testCreateSecurityGroups(){
		out.println("createSecurityGroups");
		cluster.createSecurityGroups();
		assertTrue(cluster.groupsExist());
		
	}
	/**
	 * Test for getUserData
	 */@Test
	public void testgetUserData(){
		out.println("getUserData");
		try{
			String data = cluster.getUserData();
			assertNotNull(data);
			out.println("-----USER DATA-----");
			out.println(data);
			out.println("-------------------");
		}catch(IOException e){
			fail(e.getMessage());
		}
		
	}
	/**
	 * Test of launchMaster method, of class HadoopCluster.
	 */ @Test
	public void testLaunchMaster() {
		System.out.println("launchMaster");
		try{
		RunInstancesResult result =	cluster.launchMaster(TEST_SIZE);
		out.println(result.toString());
		}catch(IOException e){
			fail("Failed to read user data.");
		}
	}
	 	/**
	 * Test of getMaster method, of class HadoopCluster.
	 */ @Test
	public void testGetMaster() {
		System.out.println("getMaster");
		ClusterInstance result = cluster.getMaster();
		assertNotNull(result);
		assertEquals(cluster.getMasterGroupName(),result.getSecurityGroups().get(0));
		Instance slaveInstance = result.getInstance();
		StringBuilder sb = new StringBuilder("\t");
		sb.append(slaveInstance.getInstanceId());
		sb.append(" ");
		sb.append(slaveInstance.getInstanceType());
		sb.append(" ");
		sb.append(slaveInstance.getLaunchTime().toString());
		sb.append(" ");
		sb.append(slaveInstance.getImageId());
		out.println(sb.toString());
	}
	/**
	 * Test of launchSlaves method, of class HadoopCluster.
	 */ @Test
	public void testLaunchSlaves() {
		System.out.println("launchSlaves");
		int howMany = 2;
		String size = "m1.large";
		try{
		RunInstancesResult result = cluster.launchSlaves(howMany, size);
		out.println(result.toString());
		}catch(IOException e){
			fail(e.getMessage());
		}
		// TODO review the generated test code and remove the default call to fail.
	}



	/**
	 * Test of getSlaves method, of class HadoopCluster.
	 */ @Test
	public void testGetSlaves() {
		out.println("getSlaves");
		List<ClusterInstance> result = cluster.getSlaves();
		out.println("found " + result.size() + " slaves for group " +TEST_GROUP);
		assertNotNull(result);
		StringBuilder sb = new StringBuilder("\t");
		for(ClusterInstance slave: result){
			assertEquals(cluster.getGroupName(),slave.getSecurityGroups().get(0));
			Instance slaveInstance = slave.getInstance();
			if(sb.length() > 1)
				sb.delete(1, sb.length());
			sb.append(slaveInstance.getInstanceId());
			sb.append(" ");
			sb.append(slaveInstance.getInstanceType());
			sb.append(" ");
			sb.append(slaveInstance.getLaunchTime().toString());
			sb.append(" ");
			sb.append(slaveInstance.getImageId());
			out.println(sb.toString());
		}
	}
	 
	/**
	 * Test of terminateCluster method, of class HadoopCluster.
	 */ @Test
	public void testTerminateCluster() {
		out.println("terminateCluster");
		out.println("skipping this test");
		/*try{
			TerminateInstancesResult tir = cluster.terminateCluster();
			if(tir != null)
				printStateChanges(tir);
		}catch(AmazonServiceException ce){
			fail("ServiceException:\nerror code: " + ce.getErrorCode()+"\ntype: "+ce.getErrorType()+"message: "+ce.getMessage());
		}catch(AmazonClientException ce){
			fail("Client Exception:\n" + ce.getMessage());
		}*/

	}

	/**
	 * Test of terminateMaster method, of class HadoopCluster.
	 */ @Test
	public void testTerminateMaster() {
		System.out.println("terminateMaster");
		out.println("skipping this test");
		//fail("The test case is a prototype.");
	}

	/**
	 * Test of terminateSlaves method, of class HadoopCluster.
	 */ @Test
	public void testTerminateSlaves() {
		System.out.println("terminateSlaves");
		out.println("skipping this test");
		//fail("The test case is a prototype.");
	}

	/**
	* Test for removeSecurityGroups
	*/ @Test
	public void testRemoveSecurityGroups(){
		out.println("removeSecurityGroups");
		cluster.removeSecurityGroups();
		assertFalse(cluster.groupsExist());
	}


	 
	private void printStateChanges(TerminateInstancesResult tir) {
		List<InstanceStateChange> states = tir.getTerminatingInstances();
		out.println("Terminated " + states.size() + " instances:");
		for(InstanceStateChange state:states){
			out.println(state.getInstanceId()+": "+state.getPreviousState()+" -> "+state.getCurrentState());
		}
	}

}