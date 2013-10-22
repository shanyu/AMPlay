package com.shanyu.hadoop.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class NormalAllocationStrategy implements AllocationStrategy {
  private static final Log LOG = LogFactory.getLog(NormalAllocationStrategy.class);
  private ApplicationMasterProtocol resourceManager;
  private int responseId = 0;
  private List<ResourceRequest> requestList = new ArrayList<ResourceRequest>();

  public NormalAllocationStrategy(ApplicationMasterProtocol resourceManager) {
    this.resourceManager = resourceManager;
  }
  
  @Override
  public void addResourceRequests(List<ResourceRequest> request) {
    requestList.addAll(request);
  }
  
  @Override
  public AllocateResponse allocateResource(float progress) throws YarnException,IOException {
    
    AllocateRequest req = Records.newRecord(AllocateRequest.class);

    // The response id set in the request will be sent back in 
    // the response so that the ApplicationMaster can 
    // match it to its original ask and act appropriately.
    req.setResponseId(responseId++);
    
    // Add the list of containers being asked for 
    if(requestList.size() == 0) {
      req.setAskList(null);
    }
    else {
      req.setAskList(requestList);
    }
    
    // If the ApplicationMaster has no need for certain 
    // containers due to over-allocation or for any other
    // reason, it can release them back to the ResourceManager
    req.setReleaseList(null);
    
    // Assuming the ApplicationMaster can track its progress
    req.setProgress(progress);
    
    AllocateResponse allocateResponse = resourceManager.allocate(req);
    
    // No need t send the same requests next time.
    requestList.clear();
    
    return allocateResponse;
  }

}
