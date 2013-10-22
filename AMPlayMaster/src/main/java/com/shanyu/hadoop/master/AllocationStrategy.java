package com.shanyu.hadoop.master;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

public interface AllocationStrategy {
  public void addResourceRequests(List<ResourceRequest> request);
  
  public AllocateResponse allocateResource(float progress) throws YarnException, IOException;
}
