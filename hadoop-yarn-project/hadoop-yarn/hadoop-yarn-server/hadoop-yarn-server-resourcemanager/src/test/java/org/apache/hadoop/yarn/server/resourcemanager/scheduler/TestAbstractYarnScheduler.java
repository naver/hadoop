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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestAbstractYarnScheduler extends ParameterizedSchedulerTestBase {

  public TestAbstractYarnScheduler(SchedulerType type) {
    super(type);
  }

  @Test
  public void testMaximimumAllocationMemory() throws Exception {
    final int node1MaxMemory = 15 * 1024;
    final int node2MaxMemory = 5 * 1024;
    final int node3MaxMemory = 6 * 1024;
    final int configuredMaxMemory = 10 * 1024;
    configureScheduler();
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        1000 * 1000);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationMemoryHelper(
          (AbstractYarnScheduler) rm.getResourceScheduler(),
          node1MaxMemory, node2MaxMemory, node3MaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory);
    } finally {
      rm.stop();
    }

    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);
    rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationMemoryHelper(
          (AbstractYarnScheduler) rm.getResourceScheduler(),
          node1MaxMemory, node2MaxMemory, node3MaxMemory,
          configuredMaxMemory, configuredMaxMemory, configuredMaxMemory,
          node2MaxMemory, node3MaxMemory, node2MaxMemory);
    } finally {
      rm.stop();
    }
  }

  private void testMaximumAllocationMemoryHelper(
       AbstractYarnScheduler scheduler,
       final int node1MaxMemory, final int node2MaxMemory,
       final int node3MaxMemory, final int... expectedMaxMemory)
       throws Exception {
    Assert.assertEquals(6, expectedMaxMemory.length);

    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    int maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[0], maxMemory);

    RMNode node1 = MockNodes.newNodeInfo(
        0, Resources.createResource(node1MaxMemory), 1, "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node1));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[1], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node1));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[2], maxMemory);

    RMNode node2 = MockNodes.newNodeInfo(
        0, Resources.createResource(node2MaxMemory), 2, "127.0.0.3");
    scheduler.handle(new NodeAddedSchedulerEvent(node2));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[3], maxMemory);

    RMNode node3 = MockNodes.newNodeInfo(
        0, Resources.createResource(node3MaxMemory), 3, "127.0.0.4");
    scheduler.handle(new NodeAddedSchedulerEvent(node3));
    Assert.assertEquals(2, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[4], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node3));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxMemory = scheduler.getMaximumResourceCapability().getMemory();
    Assert.assertEquals(expectedMaxMemory[5], maxMemory);

    scheduler.handle(new NodeRemovedSchedulerEvent(node2));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
  }

  @Test
  public void testMaximimumAllocationVCores() throws Exception {
    final int node1MaxVCores = 15;
    final int node2MaxVCores = 5;
    final int node3MaxVCores = 6;
    final int configuredMaxVCores = 10;
    configureScheduler();
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        1000 * 1000);
    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationVCoresHelper(
          (AbstractYarnScheduler) rm.getResourceScheduler(),
          node1MaxVCores, node2MaxVCores, node3MaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores);
    } finally {
      rm.stop();
    }

    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);
    rm = new MockRM(conf);
    try {
      rm.start();
      testMaximumAllocationVCoresHelper(
          (AbstractYarnScheduler) rm.getResourceScheduler(),
          node1MaxVCores, node2MaxVCores, node3MaxVCores,
          configuredMaxVCores, configuredMaxVCores, configuredMaxVCores,
          node2MaxVCores, node3MaxVCores, node2MaxVCores);
    } finally {
      rm.stop();
    }
  }

  private void testMaximumAllocationVCoresHelper(
      AbstractYarnScheduler scheduler,
      final int node1MaxVCores, final int node2MaxVCores,
      final int node3MaxVCores, final int... expectedMaxVCores)
      throws Exception {
    Assert.assertEquals(6, expectedMaxVCores.length);

    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    int maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[0], maxVCores);

    RMNode node1 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node1MaxVCores, node1MaxVCores), 1, "127.0.0.2");
    scheduler.handle(new NodeAddedSchedulerEvent(node1));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[1], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node1));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[2], maxVCores);

    RMNode node2 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node2MaxVCores, node2MaxVCores), 2, "127.0.0.3");
    scheduler.handle(new NodeAddedSchedulerEvent(node2));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[3], maxVCores);

    RMNode node3 = MockNodes.newNodeInfo(
        0, Resources.createResource(1024, node3MaxVCores, node3MaxVCores), 3, "127.0.0.4");
    scheduler.handle(new NodeAddedSchedulerEvent(node3));
    Assert.assertEquals(2, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[4], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node3));
    Assert.assertEquals(1, scheduler.getNumClusterNodes());
    maxVCores = scheduler.getMaximumResourceCapability().getVirtualCores();
    Assert.assertEquals(expectedMaxVCores[5], maxVCores);

    scheduler.handle(new NodeRemovedSchedulerEvent(node2));
    Assert.assertEquals(0, scheduler.getNumClusterNodes());
  }

  @Test
  public void testUpdateMaxAllocationUsesTotal() throws IOException {
    final int configuredMaxVCores = 20;
    final int configuredMaxMemory = 10 * 1024;
    Resource configuredMaximumResource = Resource.newInstance
        (configuredMaxMemory, configuredMaxVCores, configuredMaxVCores);

    configureScheduler();
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);

    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      AbstractYarnScheduler scheduler = (AbstractYarnScheduler) rm
          .getResourceScheduler();

      Resource emptyResource = Resource.newInstance(0, 0, 0);
      Resource fullResource1 = Resource.newInstance(1024, 5, 5);
      Resource fullResource2 = Resource.newInstance(2048, 10, 10);

      SchedulerNode mockNode1 = mock(SchedulerNode.class);
      when(mockNode1.getNodeID()).thenReturn(NodeId.newInstance("foo", 8080));
      when(mockNode1.getAvailableResource()).thenReturn(emptyResource);
      when(mockNode1.getTotalResource()).thenReturn(fullResource1);

      SchedulerNode mockNode2 = mock(SchedulerNode.class);
      when(mockNode1.getNodeID()).thenReturn(NodeId.newInstance("bar", 8081));
      when(mockNode2.getAvailableResource()).thenReturn(emptyResource);
      when(mockNode2.getTotalResource()).thenReturn(fullResource2);

      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);

      scheduler.nodes = new HashMap<NodeId, SchedulerNode>();

      scheduler.nodes.put(mockNode1.getNodeID(), mockNode1);
      scheduler.updateMaximumAllocation(mockNode1, true);
      verifyMaximumResourceCapability(fullResource1, scheduler);

      scheduler.nodes.put(mockNode2.getNodeID(), mockNode2);
      scheduler.updateMaximumAllocation(mockNode2, true);
      verifyMaximumResourceCapability(fullResource2, scheduler);

      scheduler.nodes.remove(mockNode2.getNodeID());
      scheduler.updateMaximumAllocation(mockNode2, false);
      verifyMaximumResourceCapability(fullResource1, scheduler);

      scheduler.nodes.remove(mockNode1.getNodeID());
      scheduler.updateMaximumAllocation(mockNode1, false);
      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);
    } finally {
      rm.stop();
    }
  }

  @Test
  public void testMaxAllocationAfterUpdateNodeResource() throws IOException {
    final int configuredMaxVCores = 20;
    final int configuredMaxMemory = 10 * 1024;
    Resource configuredMaximumResource = Resource.newInstance
        (configuredMaxMemory, configuredMaxVCores, configuredMaxVCores);

    configureScheduler();
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
        configuredMaxVCores);
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        configuredMaxMemory);
    conf.setLong(
        YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
        0);

    MockRM rm = new MockRM(conf);
    try {
      rm.start();
      AbstractYarnScheduler scheduler = (AbstractYarnScheduler) rm
          .getResourceScheduler();
      verifyMaximumResourceCapability(configuredMaximumResource, scheduler);

      Resource resource1 = Resource.newInstance(2048, 5, 5);
      Resource resource2 = Resource.newInstance(4096, 10, 10);
      Resource resource3 = Resource.newInstance(512, 1, 1);
      Resource resource4 = Resource.newInstance(1024, 2, 2);

      RMNode node1 = MockNodes.newNodeInfo(
          0, resource1, 1, "127.0.0.2");
      scheduler.handle(new NodeAddedSchedulerEvent(node1));
      RMNode node2 = MockNodes.newNodeInfo(
          0, resource3, 2, "127.0.0.3");
      scheduler.handle(new NodeAddedSchedulerEvent(node2));
      verifyMaximumResourceCapability(resource1, scheduler);

      // increase node1 resource
      scheduler.updateNodeResource(node1, ResourceOption.newInstance(
          resource2, 0));
      verifyMaximumResourceCapability(resource2, scheduler);

      // decrease node1 resource
      scheduler.updateNodeResource(node1, ResourceOption.newInstance(
          resource1, 0));
      verifyMaximumResourceCapability(resource1, scheduler);

      // increase node2 resource
      scheduler.updateNodeResource(node2, ResourceOption.newInstance(
          resource4, 0));
      verifyMaximumResourceCapability(resource1, scheduler);

      // decrease node2 resource
      scheduler.updateNodeResource(node2, ResourceOption.newInstance(
          resource3, 0));
      verifyMaximumResourceCapability(resource1, scheduler);
    } finally {
      rm.stop();
    }
  }

  private void verifyMaximumResourceCapability(
      Resource expectedMaximumResource, AbstractYarnScheduler scheduler) {

    final Resource schedulerMaximumResourceCapability = scheduler
        .getMaximumResourceCapability();
    Assert.assertEquals(expectedMaximumResource.getMemory(),
        schedulerMaximumResourceCapability.getMemory());
    Assert.assertEquals(expectedMaximumResource.getVirtualCores(),
        schedulerMaximumResourceCapability.getVirtualCores());
  }
}
