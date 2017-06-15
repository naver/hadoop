/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.webapp.log;

import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.PRE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class AggregatedLogsBlock extends HtmlBlock {

  private final Configuration conf;

  @Inject
  AggregatedLogsBlock(Configuration conf) {
    this.conf = conf;
  }

  public static List<Path> getRemoteAppLogDirs(Configuration conf, ApplicationId appId, String appOwner) throws UnsupportedFileSystemException {

    Set<Path> remoteRootLogDirs = new HashSet<Path>();
    List<Path> remoteAppLogDirs = new ArrayList<Path>();

    Path remoteRootLogDir = new Path(conf.get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));

    Collection<String> logDirs = conf.getTrimmedStringCollection(YarnConfiguration.NM_REMOTE_APP_LOG_DIRS);

    FileContext fc = FileContext.getFileContext(conf);
    remoteRootLogDir = fc.makeQualified(remoteRootLogDir);

    remoteRootLogDirs.add(remoteRootLogDir);
    for(String dir: logDirs){
      Path d = fc.makeQualified(new Path(dir));
      remoteRootLogDirs.add(d);
    }

    String user = appOwner;
    String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);

    for(Path dir: remoteRootLogDirs) {
      Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
          dir, appId, user, logDirSuffix);

      remoteAppLogDirs.add(remoteAppLogDir);
    }

    return remoteAppLogDirs;
  }

  public static RemoteIterator<FileStatus> getFileListAtRemoteAppDir(Configuration conf, List<Path> remoteAppLogDirs, ApplicationId appId, String appOwner) throws IOException {

    if(remoteAppLogDirs == null) {
      remoteAppLogDirs = getRemoteAppLogDirs(conf, appId, appOwner);
    }
    final List<FileStatus> mergedStatus = new ArrayList<>();

    int fnf = 0;
    int ioe = 0;
    StringBuffer sb = new StringBuffer();

    for(Path remoteAppDir: remoteAppLogDirs){
      try {

        FileSystem fs = remoteAppDir.getFileSystem(conf);
        FileStatus[] status = fs.listStatus(remoteAppDir);

        for(FileStatus s: status){
          mergedStatus.add(s);
        }

      } catch(FileNotFoundException e){
        fnf++;
      } catch(IOException e){
        ioe++;
        if(sb.length() > 0){
          sb.append("\n");
        }
        sb.append(e.getMessage());
      }
    }

    if(remoteAppLogDirs.size() == fnf){
      throw new FileNotFoundException(StringUtils.join(remoteAppLogDirs, ","));
    }
    if(remoteAppLogDirs.size() == ioe){
      throw new IOException(sb.toString());
    }

    return new RemoteIterator<FileStatus>() {
      private int i = 0;
      private List<FileStatus> statusList = mergedStatus;

      @Override
      public boolean hasNext() {
        return i < statusList.size();
      }

      @Override
      public FileStatus next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return statusList.get(i++);
      }
    };

  }

  @Override
  protected void render(Block html) {
    ContainerId containerId = verifyAndGetContainerId(html);
    NodeId nodeId = verifyAndGetNodeId(html);
    String appOwner = verifyAndGetAppOwner(html);
    LogLimits logLimits = verifyAndGetLogLimits(html);
    if (containerId == null || nodeId == null || appOwner == null
        || appOwner.isEmpty() || logLimits == null) {
      return;
    }

    ApplicationId applicationId = containerId.getApplicationAttemptId()
        .getApplicationId();
    String logEntity = $(ENTITY_STRING);
    if (logEntity == null || logEntity.isEmpty()) {
      logEntity = containerId.toString();
    }

    if (!conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      html.h1()
          ._("Aggregation is not enabled. Try the nodemanager at " + nodeId)
          ._();
      return;
    }


    RemoteIterator<FileStatus> nodeFiles;
    try {
      nodeFiles = getFileListAtRemoteAppDir(conf, null, applicationId, appOwner);
    } catch (FileNotFoundException fnf) {
      html.h1()
          ._("Logs not available for " + logEntity
              + ". Aggregation may not be complete, "
              + "Check back later or try the nodemanager at " + nodeId)._();
      return;
    } catch (Exception ex) {
      html.h1()
          ._("Error getting logs at " + nodeId)._();
      return;
    }

    boolean foundLog = false;
    String desiredLogType = $(CONTAINER_LOG_TYPE);
    try {
      while (nodeFiles.hasNext()) {
        AggregatedLogFormat.LogReader reader = null;
        try {
          FileStatus thisNodeFile = nodeFiles.next();
          if (!thisNodeFile.getPath().getName()
            .contains(LogAggregationUtils.getNodeString(nodeId))
              || thisNodeFile.getPath().getName()
                .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
            continue;
          }
          long logUploadedTime = thisNodeFile.getModificationTime();
          reader =
              new AggregatedLogFormat.LogReader(conf, thisNodeFile.getPath());

          String owner = null;
          Map<ApplicationAccessType, String> appAcls = null;
          try {
            owner = reader.getApplicationOwner();
            appAcls = reader.getApplicationAcls();
          } catch (IOException e) {
            LOG.error("Error getting logs for " + logEntity, e);
            continue;
          }
          ApplicationACLsManager aclsManager = new ApplicationACLsManager(conf);
          aclsManager.addApplication(applicationId, appAcls);

          String remoteUser = request().getRemoteUser();
          UserGroupInformation callerUGI = null;
          if (remoteUser != null) {
            callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
          }
          if (callerUGI != null && !aclsManager.checkAccess(callerUGI,
              ApplicationAccessType.VIEW_APP, owner, applicationId)) {
            html.h1()
                ._("User [" + remoteUser
                    + "] is not authorized to view the logs for " + logEntity
                    + " in log file [" + thisNodeFile.getPath().getName() + "]")._();
            LOG.error("User [" + remoteUser
              + "] is not authorized to view the logs for " + logEntity);
            continue;
          }

          AggregatedLogFormat.ContainerLogsReader logReader = reader
            .getContainerLogsReader(containerId);
          if (logReader == null) {
            continue;
          }

          foundLog = readContainerLogs(html, logReader, logLimits,
              desiredLogType, logUploadedTime);
        } catch (IOException ex) {
          LOG.error("Error getting logs for " + logEntity, ex);
          continue;
        } finally {
          if (reader != null)
            reader.close();
        }
      }
      if (!foundLog) {
        if (desiredLogType.isEmpty()) {
          html.h1("No logs available for container " + containerId.toString());
        } else {
          html.h1("Unable to locate '" + desiredLogType
              + "' log for container " + containerId.toString());
        }
      }
    } catch (IOException e) {
      html.h1()._("Error getting logs for " + logEntity)._();
      LOG.error("Error getting logs for " + logEntity, e);
    }
  }

  private boolean readContainerLogs(Block html,
      AggregatedLogFormat.ContainerLogsReader logReader, LogLimits logLimits,
      String desiredLogType, long logUpLoadTime) throws IOException {
    int bufferSize = 65536;
    char[] cbuf = new char[bufferSize];

    boolean foundLog = false;
    String logType = logReader.nextLog();
    while (logType != null) {
      if (desiredLogType == null || desiredLogType.isEmpty()
          || desiredLogType.equals(logType)) {
        long logLength = logReader.getCurrentLogLength();
        if (foundLog) {
          html.pre()._("\n\n")._();
        }

        html.p()._("Log Type: " + logType)._();
        html.p()._("Log Upload Time: " + Times.format(logUpLoadTime))._();
        html.p()._("Log Length: " + Long.toString(logLength))._();

        long start = logLimits.start < 0
            ? logLength + logLimits.start : logLimits.start;
        start = start < 0 ? 0 : start;
        start = start > logLength ? logLength : start;
        long end = logLimits.end < 0
            ? logLength + logLimits.end : logLimits.end;
        end = end < 0 ? 0 : end;
        end = end > logLength ? logLength : end;
        end = end < start ? start : end;

        long toRead = end - start;
        if (toRead < logLength) {
            html.p()._("Showing " + toRead + " bytes of " + logLength
                + " total. Click ")
                .a(url("logs", $(NM_NODENAME), $(CONTAINER_ID),
                    $(ENTITY_STRING), $(APP_OWNER),
                    logType, "?start=0"), "here").
                    _(" for the full log.")._();
        }

        long totalSkipped = 0;
        while (totalSkipped < start) {
          long ret = logReader.skip(start - totalSkipped);
          if (ret == 0) {
            //Read one byte
            int nextByte = logReader.read();
            // Check if we have reached EOF
            if (nextByte == -1) {
              throw new IOException( "Premature EOF from container log");
            }
            ret = 1;
          }
          totalSkipped += ret;
        }

        int len = 0;
        int currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
        PRE<Hamlet> pre = html.pre();

        while (toRead > 0
            && (len = logReader.read(cbuf, 0, currentToRead)) > 0) {
          pre._(new String(cbuf, 0, len));
          toRead = toRead - len;
          currentToRead = toRead > bufferSize ? bufferSize : (int) toRead;
        }

        pre._();
        foundLog = true;
      }

      logType = logReader.nextLog();
    }

    return foundLog;
  }

  private ContainerId verifyAndGetContainerId(Block html) {
    String containerIdStr = $(CONTAINER_ID);
    if (containerIdStr == null || containerIdStr.isEmpty()) {
      html.h1()._("Cannot get container logs without a ContainerId")._();
      return null;
    }
    ContainerId containerId = null;
    try {
      containerId = ConverterUtils.toContainerId(containerIdStr);
    } catch (IllegalArgumentException e) {
      html.h1()
          ._("Cannot get container logs for invalid containerId: "
              + containerIdStr)._();
      return null;
    }
    return containerId;
  }

  private NodeId verifyAndGetNodeId(Block html) {
    String nodeIdStr = $(NM_NODENAME);
    if (nodeIdStr == null || nodeIdStr.isEmpty()) {
      html.h1()._("Cannot get container logs without a NodeId")._();
      return null;
    }
    NodeId nodeId = null;
    try {
      nodeId = ConverterUtils.toNodeId(nodeIdStr);
    } catch (IllegalArgumentException e) {
      html.h1()._("Cannot get container logs. Invalid nodeId: " + nodeIdStr)
          ._();
      return null;
    }
    return nodeId;
  }
  
  private String verifyAndGetAppOwner(Block html) {
    String appOwner = $(APP_OWNER);
    if (appOwner == null || appOwner.isEmpty()) {
      html.h1()._("Cannot get container logs without an app owner")._();
    }
    return appOwner;
  }

  private static class LogLimits {
    long start;
    long end;
  }

  private LogLimits verifyAndGetLogLimits(Block html) {
    long start = -4096;
    long end = Long.MAX_VALUE;
    boolean isValid = true;

    String startStr = $("start");
    if (startStr != null && !startStr.isEmpty()) {
      try {
        start = Long.parseLong(startStr);
      } catch (NumberFormatException e) {
        isValid = false;
        html.h1()._("Invalid log start value: " + startStr)._();
      }
    }

    String endStr = $("end");
    if (endStr != null && !endStr.isEmpty()) {
      try {
        end = Long.parseLong(endStr);
      } catch (NumberFormatException e) {
        isValid = false;
        html.h1()._("Invalid log end value: " + endStr)._();
      }
    }

    if (!isValid) {
      return null;
    }

    LogLimits limits = new LogLimits();
    limits.start = start;
    limits.end = end;
    return limits;
  }
}