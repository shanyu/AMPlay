package com.shanyu.hadoop.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;


/**
 * ApplicationMater
 *
 */
public class AppMaster extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(AppMaster.class);
  
  public AppMaster () {
  }
  
  @Override
  public int run(String[] args) throws Exception {
    LOG.info("Running AppMaster with num of args: " + args.length);
        
    AMService service = new AMService(getConf(), Integer.parseInt(args[0]));
    int timeoutInSec = Integer.parseInt(args[1]);
    int runningSec = 0;
    
    service.startAndWait();
    while(service.hasContainerRunning() && runningSec < timeoutInSec) {
      Thread.sleep(5000);
      runningSec += 5;
    }
    service.stopAndWait();
    LOG.info("AppMaster done");
    
    return 0;
  }
  
  
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new AppMaster(), args);
    System.exit(rc);
  }

}
