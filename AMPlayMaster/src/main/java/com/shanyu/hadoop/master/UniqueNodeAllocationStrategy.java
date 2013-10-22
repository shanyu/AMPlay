package com.shanyu.hadoop.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class UniqueNodeAllocationStrategy implements AllocationStrategy {
  private static final Log LOG = LogFactory.getLog(UniqueNodeAllocationStrategy.class);
  private ApplicationMasterProtocol resourceManager;
  private int responseId = 0;
  
  private List<ResourceRequest> requestList = new ArrayList<ResourceRequest>();
  private List<ContainerId> releaseList = new ArrayList<ContainerId>();
  private List<Container> releaseContainerList = new ArrayList<Container>();
  private Set<String> blacklistSet = new HashSet<String>();
  private List<String> blacklist = new ArrayList<String>();

  public UniqueNodeAllocationStrategy(ApplicationMasterProtocol resourceManager){
    this.resourceManager = resourceManager;
  }
  
  @Override
  public void addResourceRequests(List<ResourceRequest> request) {
    requestList.addAll(request);
  }

  @Override
  public AllocateResponse allocateResource(float progress)
      throws YarnException, IOException {
    AllocateRequest req = Records.newRecord(AllocateRequest.class);
    req.setResponseId(responseId++);
    
    req.setAskList(requestList);
    req.setReleaseList(releaseList);
    if(blacklist.size() > 0) {
      ResourceBlacklistRequest br = ResourceBlacklistRequest.newInstance(blacklist, null);
      req.setResourceBlacklistRequest(br);
    }
    
    req.setProgress(progress);
    
    AllocateResponse allocateResponse = resourceManager.allocate(req);
    
    // Only allow unique nodes
    for(Container container : allocateResponse.getAllocatedContainers()) {
      String host = container.getNodeId().getHost();
      if(blacklistSet.contains(host)) {
        //duplicated nodes
        releaseList.add(container.getId());
        releaseContainerList.add(container);
        LOG.info("Adding to releaseList: " + host + " / " + container.getId());
      }
      else {
        LOG.info("Adding to blacklist: " + host);
        blacklistSet.add(host);
        blacklist.add(host);
      }
    }
    
    if(releaseContainerList.size() > 0) {
      //need to modify AllocateResponse to remove containers to be released.
      List<Container> cl = allocateResponse.getAllocatedContainers();
      for(Container container : releaseContainerList) {
        cl.remove(container);
      }
    }
    
    // No need t send the same requests next time.
    requestList.clear();
    releaseList.clear();
    releaseContainerList.clear();
    
    return allocateResponse;
  }

}
