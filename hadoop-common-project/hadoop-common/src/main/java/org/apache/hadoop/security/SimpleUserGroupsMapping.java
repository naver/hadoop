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
package org.apache.hadoop.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;



import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


/**
 * A simple implementation of {@link GroupMappingServiceProvider}
 * that group is a given user.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SimpleUserGroupsMapping
    implements GroupMappingServiceProvider {

  private static final Log LOG =
      LogFactory.getLog(SimpleUserGroupsMapping.class);

  /**
   * Returns list of groups for a user.
   *
   * @param user get groups for this user
   * @return list of groups for a given user
   */
  @Override
  public List<String> getGroups(String user) throws IOException {

    List<String> groups = new LinkedList<String>();
    groups.add(user);

    return groups;
  }

  /**
   * Caches groups, no need to do that for this provider.
   */
  @Override
  public void cacheGroupsRefresh() throws IOException {
    // does nothing in this provider of user to groups mapping
  }

  /**
   * Adds groups to cache, no need to do that for this provider.
   *
   * @param groups unused
   */
  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }

}
