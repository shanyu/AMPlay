package com.shanyu.hadoop.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.client.api.NMTokenCache;

import com.google.common.util.concurrent.AbstractScheduledService;

//TBD: need a container service to stop container!!

public class AMService extends AbstractScheduledService {
  private static final Log LOG = LogFactory.getLog(AMService.class);
  
  private Configuration conf;
  private YarnRPC rpc;
  
  private int workerMemory = 1024;
  private String shellScriptUri = "/amplay/MyExecShell.cmd";
  private int heartbeatInterval = 10;
  
  private ApplicationAttemptId appAttemptID;
  private ApplicationMasterProtocol resourceManager;
  private int responseId = 0;
  private int numContainers = 2;
  private int numCompletedContainers = 0;
  private int currentProgress = 0;
  
  public AMService (Configuration c, int numC) {
    conf = c;
    rpc = YarnRPC.create(conf);
    numContainers = numC;
    LOG.info("numContainers is set to " + numContainers);
  }
  
  protected void startUp() throws Exception {
    getAttemptId();
    connect();
    registerAM();
    
    List<ResourceRequest> requestedContainers = new ArrayList<ResourceRequest>();
    requestedContainers.add(setupResourceRequest());
    allocateResource(requestedContainers, null);
  }

  protected void runOneIteration() throws Exception {
    LOG.info("runOneIteration, attemp ID: " + appAttemptID);
    AllocateResponse allocateResponse = allocateResource(null, null);
    handleAllocation(allocateResponse);
  }
  
  protected void shutDown() throws Exception {
    finishUp();
  }

  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, heartbeatInterval, TimeUnit.SECONDS);
  }
  
  public boolean hasContainerRunning() {
    if(numContainers == numCompletedContainers) {
      return false;
    }
    else {
      return true;
    }
  }
  
  ///////////////////////////////////////////////////////////////////////////
  //Private methods
  
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
    appAttemptID = containerId.getApplicationAttemptId();
    LOG.info("ApplicationAttemptId = " + appAttemptID);
  }
  
  private void connect() throws IOException {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    final InetSocketAddress rmAddress = 
        NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
    LOG.info("Connecting to ResourceManager Scheduler at " + rmAddress);
    
    for (Token<? extends TokenIdentifier> token : UserGroupInformation
        .getCurrentUser().getTokens()) {
      LOG.info("found token: " + token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        SecurityUtil.setTokenService(token, rmAddress);
      }
    }

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
    
    //RegisterApplicationMasterRequest appMasterRequest = 
    //    Records.newRecord(RegisterApplicationMasterRequest.class);
    RegisterApplicationMasterRequest appMasterRequest =
        RegisterApplicationMasterRequest.newInstance("localhost", 8888, "localhost:8888");
    
    // The registration response is useful as it provides information about the 
    // cluster. 
    // Similar to the GetNewApplicationResponse in the client, it provides 
    // information about the min/mx resource capabilities of the cluster that 
    // would be needed by the ApplicationMaster when requesting for containers.
    RegisterApplicationMasterResponse response = 
        resourceManager.registerApplicationMaster(appMasterRequest);
    
    LOG.info("Register AM, response : " + response.toString());
  }
  
  private ResourceRequest setupResourceRequest() {
    // Resource Request
    ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);

    // setup requirements for hosts 
    // whether a particular rack/host is needed 
    // useful for applications that are sensitive
    // to data locality 
    rsrcRequest.setResourceName("*");

    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    rsrcRequest.setPriority(pri);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(workerMemory);
    rsrcRequest.setCapability(capability);

    // set no. of containers needed
    // matching the specifications
    rsrcRequest.setNumContainers(numContainers);
    
    return rsrcRequest;
  }
  
  private AllocateResponse allocateResource(List<ResourceRequest> request,
      List<ContainerId> release) throws IOException,YarnException {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);

    // The response id set in the request will be sent back in 
    // the response so that the ApplicationMaster can 
    // match it to its original ask and act appropriately.
    req.setResponseId(responseId++);
    
    // Add the list of containers being asked for 
    req.setAskList(request);
    
    // If the ApplicationMaster has no need for certain 
    // containers due to over-allocation or for any other
    // reason, it can release them back to the ResourceManager
    req.setReleaseList(release);
    
    // Assuming the ApplicationMaster can track its progress
    req.setProgress(currentProgress);
    
    AllocateResponse allocateResponse = resourceManager.allocate(req);
    populateNMTokens(allocateResponse);
    
    LOG.info("allocateResponse ResponseId: " + allocateResponse.getResponseId());
    LOG.info("allocateResponse available memory: " + allocateResponse.getAvailableResources().getMemory());
    LOG.info("allocateResponse allocated container: " + allocateResponse.getAllocatedContainers().size());
    LOG.info("allocateResponse completed container: " + allocateResponse.getCompletedContainersStatuses().size());
    
    return allocateResponse;
    
  }
  
  private void populateNMTokens(AllocateResponse allocateResponse) {
    LOG.info("populateNMTokens()");
    for (NMToken token : allocateResponse.getNMTokens()) {
      String nodeId = token.getNodeId().toString();
      if (NMTokenCache.containsNMToken(nodeId)) {
        LOG.info("Replacing token for : " + nodeId);
      } else {
        LOG.info("Received new token for : " + nodeId);
      }
      NMTokenCache.setNMToken(nodeId, token.getToken());
    }
  }
  
  private void handleAllocation(AllocateResponse response) throws IOException,YarnException {
    for(Container container : response.getAllocatedContainers()) {
      startContainer(container);
    }
    
    for (ContainerStatus status : response.getCompletedContainersStatuses()) {
      numCompletedContainers++;
      int exitStatus = status.getExitStatus();
      LOG.info("container " + status.getContainerId() + " finished with exit status: " + exitStatus);
    }
  }
  
  private void startContainer(Container container) throws IOException,YarnException {
    LOG.info("starting container: contaner ID: " + container.getId());
    // Connect to ContainerManager on the allocated container 
    String cmIpPortStr = container.getNodeId().getHost() + ":" 
        + container.getNodeId().getPort();
    final InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);     
    LOG.info("connecting to " + cmAddress);
    
    UserGroupInformation user = UserGroupInformation.createRemoteUser(
        container.getId().getApplicationAttemptId().toString());
    user.addToken(ConverterUtils.convertFromYarn(NMTokenCache.getNMToken(container.getNodeId().toString()), cmAddress));
    ContainerManagementProtocol cm = user
        .doAs(new PrivilegedAction<ContainerManagementProtocol>() {
          @Override
          public ContainerManagementProtocol run() {
            return (ContainerManagementProtocol) rpc.getProxy(ContainerManagementProtocol.class, cmAddress, conf);
          }
        });
    
    // Now we setup a ContainerLaunchContext  
    ContainerLaunchContext ctx = 
        Records.newRecord(ContainerLaunchContext.class);

    // Set the local resources 
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    // Again, the local resources from the ApplicationMaster is not copied over 
    // by default to the allocated container. Thus, it is the responsibility 
    // of the ApplicationMaster to setup all the necessary local resources 
    // needed by the job that will be executed on the allocated container. 
      
    // Assume that we are executing a shell script on the allocated container 
    // and the shell script's location in the filesystem is known to us. 
    FileSystem fs = FileSystem.get(conf);
    Path shellScriptPath = fs.makeQualified(new Path(shellScriptUri)); // <- known path to jar file
    FileStatus pathStatus = fs.getFileStatus(shellScriptPath);
    LocalResource shellRsrc = Records.newRecord(LocalResource.class);
    shellRsrc.setType(LocalResourceType.FILE);
    shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);          
    shellRsrc.setResource(
        ConverterUtils.getYarnUrlFromPath(shellScriptPath));
    shellRsrc.setTimestamp(pathStatus.getModificationTime());
    shellRsrc.setSize(pathStatus.getLen());
    localResources.put("MyExecShell.cmd", shellRsrc);

    ctx.setLocalResources(localResources);                      

    // Set the necessary command to execute on the allocated container 
    String command = "cmd /c MyExecShell.cmd"
        + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
        + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

    List<String> commands = new ArrayList<String>();
    commands.add(command);
    ctx.setCommands(commands);

    // Send the start request to the ContainerManager
    StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
    startReq.setContainerLaunchContext(ctx);
    LOG.info("container token: " + container.getContainerToken());
    startReq.setContainerToken(container.getContainerToken());
    List<StartContainerRequest> reqs = new ArrayList<StartContainerRequest>();
    reqs.add(startReq);
    
    StartContainersRequest startReqs = Records.newRecord(StartContainersRequest.class);
    startReqs.setStartContainerRequests(reqs);
    LOG.info("about to start containers");
    cm.startContainers(startReqs);
  }
  
  private void finishUp() throws IOException, YarnException {
    FinishApplicationMasterRequest appFinishRequest = 
        Records.newRecord(FinishApplicationMasterRequest.class);
    appFinishRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    resourceManager.finishApplicationMaster(appFinishRequest);
    LOG.info("AppService finished");
  }

}
