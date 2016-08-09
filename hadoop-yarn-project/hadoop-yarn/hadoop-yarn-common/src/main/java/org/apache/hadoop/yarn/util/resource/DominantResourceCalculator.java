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
package org.apache.hadoop.yarn.util.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Arrays;

/**
 * A {@link ResourceCalculator} which uses the concept of  
 * <em>dominant resource</em> to compare multi-dimensional resources.
 *
 * Essentially the idea is that the in a multi-resource environment, 
 * the resource allocation should be determined by the dominant share 
 * of an entity (user or queue), which is the maximum share that the 
 * entity has been allocated of any resource. 
 * 
 * In a nutshell, it seeks to maximize the minimum dominant share across 
 * all entities. 
 * 
 * For example, if user A runs CPU-heavy tasks and user B runs
 * memory-heavy tasks, it attempts to equalize CPU share of user A 
 * with Memory-share of user B. 
 * 
 * In the single resource case, it reduces to max-min fairness for that resource.
 * 
 * See the Dominant Resource Fairness paper for more details:
 * www.cs.berkeley.edu/~matei/papers/2011/nsdi_drf.pdf
 */
@Private
@Unstable
public class DominantResourceCalculator extends ResourceCalculator {

  private static final Log LOG = LogFactory.getLog(DominantResourceCalculator.class);

  @Override
  public int compare(Resource clusterResource, Resource lhs, Resource rhs) {
    
    if (lhs.equals(rhs)) {
      return 0;
    }

    float[] lValues = new float[] {
      (clusterResource.getMemory() != 0) ? (float) lhs.getMemory() / clusterResource.getMemory() : lhs.getMemory(),
      (clusterResource.getVirtualCores() != 0) ? (float) lhs.getVirtualCores() / clusterResource.getVirtualCores() : lhs.getVirtualCores(),
      (clusterResource.getGpuCores() != 0) ? (float) lhs.getGpuCores() / clusterResource.getGpuCores() : 0.0f };
    Arrays.sort(lValues);

    float[] rValues = new float[] {
      (clusterResource.getMemory() != 0) ? (float) rhs.getMemory() / clusterResource.getMemory() : rhs.getMemory(),
      (clusterResource.getVirtualCores() != 0) ? (float) rhs.getVirtualCores() / clusterResource.getVirtualCores() : rhs.getVirtualCores(),
      (clusterResource.getGpuCores() != 0) ? (float) rhs.getGpuCores() / clusterResource.getGpuCores() : 0.0f };
    Arrays.sort(rValues);

    int diff = 0;
    for(int i = 0; i < 3; i++) {
      float l = lValues[i];
      float r = rValues[i];
      if (l < r) {
        diff = -1;
      } else if (l > r) {
        diff = 1;
      }
    }
    
    return diff;
  }

  protected float getResourceAsValueMax( Resource clusterResource,
      Resource resource) {
    return Math.max((float) resource.getMemory() / clusterResource.getMemory(),
        (float) resource.getVirtualCores() / clusterResource.getVirtualCores());
  }

  @Override
  public int computeAvailableContainers(Resource available, Resource required) {
    int min = Math.min(
        available.getMemory() / required.getMemory(),
        available.getVirtualCores() / required.getVirtualCores());
    if (required.getGpuCores() != 0) {
      min = Math.min(min,
          available.getGpuCores() / required.getGpuCores());
    }
    return min;
  }

  @Override
  public float divide(Resource clusterResource, 
      Resource numerator, Resource denominator) {
    return 
        getResourceAsValueMax(clusterResource, numerator) /
        getResourceAsValueMax(clusterResource, denominator);
  }
  
  @Override
  public boolean isInvalidDivisor(Resource r) {
    if (r.getMemory() == 0.0f || r.getVirtualCores() == 0.0f || r.getGpuCores() == 0.0f) {
      return true;
    }
    return false;
  }

  @Override
  public float ratio(Resource a, Resource b) {
    float max = Math.max(
        (float) a.getMemory() / b.getMemory(),
        (float) a.getVirtualCores() / b.getVirtualCores());
    if (b.getGpuCores() != 0) {
      max = Math.max(max,
          (float) a.getGpuCores() / b.getGpuCores());
    }
    return max;
  }

  @Override
  public Resource divideAndCeil(Resource numerator, int denominator) {
    return Resources.createResource(
        divideAndCeil(numerator.getMemory(), denominator),
        divideAndCeil(numerator.getVirtualCores(), denominator),
        divideAndCeil(numerator.getGpuCores(), denominator)
        );
  }

  @Override
  public Resource normalize(Resource r, Resource minimumResource,
                            Resource maximumResource, Resource stepFactor) {
    int normalizedMemory = Math.min(
      roundUp(
        Math.max(r.getMemory(), minimumResource.getMemory()),
        stepFactor.getMemory()),
      maximumResource.getMemory());
    int normalizedCores = Math.min(
      roundUp(
        Math.max(r.getVirtualCores(), minimumResource.getVirtualCores()),
        stepFactor.getVirtualCores()),
      maximumResource.getVirtualCores());
    int normalizedGCores = Math.min(
      roundUpWithZero(
        Math.max(r.getGpuCores(), minimumResource.getGpuCores()),
        stepFactor.getGpuCores()),
      maximumResource.getGpuCores());
    return Resources.createResource(normalizedMemory,
      normalizedCores, normalizedGCores);
  }

  @Override
  public Resource roundUp(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundUp(r.getMemory(), stepFactor.getMemory()), 
        roundUp(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundUpWithZero(r.getGpuCores(), stepFactor.getGpuCores())
        );
  }

  @Override
  public Resource roundDown(Resource r, Resource stepFactor) {
    return Resources.createResource(
        roundDown(r.getMemory(), stepFactor.getMemory()),
        roundDown(r.getVirtualCores(), stepFactor.getVirtualCores()),
        roundDownWithZero(r.getGpuCores(), stepFactor.getGpuCores())
        );
  }

  @Override
  public Resource multiplyAndNormalizeUp(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundUp(
            (int)Math.ceil(r.getMemory() * by), stepFactor.getMemory()),
        roundUp(
            (int)Math.ceil(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()),
        roundUpWithZero(
            (int)Math.ceil(r.getGpuCores() * by),
            stepFactor.getGpuCores())
        );
  }

  @Override
  public Resource multiplyAndNormalizeDown(Resource r, double by,
      Resource stepFactor) {
    return Resources.createResource(
        roundDown(
            (int)(r.getMemory() * by), 
            stepFactor.getMemory()
            ),
        roundDown(
            (int)(r.getVirtualCores() * by), 
            stepFactor.getVirtualCores()
        ),
        roundDownWithZero(
            (int) (r.getGpuCores() * by),
            stepFactor.getGpuCores()
            )
        );
  }

}
