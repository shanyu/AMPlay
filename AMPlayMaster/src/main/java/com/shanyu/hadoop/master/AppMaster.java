package com.shanyu.hadoop.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


/**
 * ApplicationMater
 *
 */
public class AppMaster extends Configured implements Tool {
  
  private static final Log LOG = LogFactory.getLog(AppMaster.class);
  
  private Configuration conf;
  private YarnRPC rpc;
  
  private ApplicationAttemptId appAttemptID;
  private ApplicationMasterProtocol resourceManager;
  
  public AppMaster () {
  }
  
  @Override
  public int run(String[] args) throws Exception {
    LOG.info("Running AppMaster with: " + args.length);
    init();
    getAttemptId();
    connect();
    registerAM();
    
    //need to use allocate() to emit heartbeat
    Thread.sleep(60000);
    
    finishUp();
    return 0;
  }
  
  private void init() {
    conf = getConf();
    rpc = YarnRPC.create(conf);
  }
  
  private void getAttemptId() {
    Map<String, String> envs = System.getenv();
    String containerIdString = 
        envs.get(ApplicationConstants.Environment.CONTAINER_ID.toString());
    if (containerIdString == null) {
      // container id should always be set in the env by the framework 
      throw new IllegalArgumentException(
          "ContainerId not set in the environment");
    }
    ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
    ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
    LOG.info("ApplicationAttemptId = " + appAttemptID);
  }
  
  private void connect() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = 
        NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));             
    LOG.info("Connecting to ResourceManager Scheduler at " + rmAddress);

    resourceManager = (ApplicationMasterProtocol) rpc.getProxy(
        ApplicationMasterProtocol.class, rmAddress, conf);
  }
  
  private void registerAM() throws IOException,YarnException{
        
    // Register the AM with the RM
    // Set the required info into the registration request: 
    // ApplicationAttemptId, 
    // host on which the app master is running
    // rpc port on which the app master accepts requests from the client 
    // tracking url for the client to track app master progress
    RegisterApplicationMasterRequest appMasterRequest = 
        Records.newRecord(RegisterApplicationMasterRequest.class);   
    
    InetSocketAddress serviceAddr = null;
    serviceAddr = new InetSocketAddress("localhost", 8888);
    appMasterRequest.setHost(serviceAddr.getHostName());
    appMasterRequest.setRpcPort(serviceAddr.getPort());
    appMasterRequest.setTrackingUrl(serviceAddr.getHostName() + ":" + serviceAddr.getPort());
    
    // The registration response is useful as it provides information about the 
    // cluster. 
    // Similar to the GetNewApplicationResponse in the client, it provides 
    // information about the min/mx resource capabilities of the cluster that 
    // would be needed by the ApplicationMaster when requesting for containers.
    RegisterApplicationMasterResponse response = 
        resourceManager.registerApplicationMaster(appMasterRequest);
    
    LOG.info("Registered AM: response=" + response.toString());
  }
  
  private void finishUp() throws IOException, YarnException {
    FinishApplicationMasterRequest appFinishRequest = 
        Records.newRecord(FinishApplicationMasterRequest.class);
    appFinishRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    resourceManager.finishApplicationMaster(appFinishRequest);
    LOG.info("AppMaster finished");
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new AppMaster(), args);
    System.exit(rc);
  }

  /*
  private void addResourceRequest(Priority priority, String resourceName,
      Resource capability) {
    Map<String, Map<Resource, ResourceRequest>> remoteRequests =
      this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, Map<Resource, ResourceRequest>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Added priority=" + priority);
      }
    }
    Map<Resource, ResourceRequest> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new HashMap<Resource, ResourceRequest>();
      remoteRequests.put(resourceName, reqMap);
    }
    ResourceRequest remoteRequest = reqMap.get(capability);
    if (remoteRequest == null) {
      remoteRequest = recordFactory.newRecordInstance(ResourceRequest.class);
      remoteRequest.setPriority(priority);
      remoteRequest.setResourceName(resourceName);
      remoteRequest.setCapability(capability);
      remoteRequest.setNumContainers(0);
      reqMap.put(capability, remoteRequest);
    }
    remoteRequest.setNumContainers(remoteRequest.getNumContainers() + 1);

    // Note this down for next interaction with ResourceManager
    addResourceRequestToAsk(remoteRequest);
    if (LOG.isDebugEnabled()) {
      LOG.debug("addResourceRequest:" + " applicationId="
          + applicationId.getId() + " priority=" + priority.getPriority()
          + " resourceName=" + resourceName + " numContainers="
          + remoteRequest.getNumContainers() + " #asks=" + ask.size());
    }
  }
  */
}
