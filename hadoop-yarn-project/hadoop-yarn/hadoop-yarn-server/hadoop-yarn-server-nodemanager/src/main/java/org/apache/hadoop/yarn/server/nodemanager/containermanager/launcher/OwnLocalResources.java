package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

public class OwnLocalResources {

  private static final Log LOG = LogFactory.getLog(OwnLocalResources.class);

  public static final String LR_PREFIX = "_native_";
  public static final String LR_TAG_CENTOS6 = "centos6";
  public static final String LR_TAG_CENTOS7 = "centos7";
  public static final String LR_TAG_MACOS10 = "macos10";
  public static final String LR_TAG_DELIM = "_";
  /*
  _native_centos7_liba.so
  liba.so
  _native_centos6_liba.so
   */

  private static final Set<String> LR_ALL_TAGS = getLocalResourcesAllTags();
  private final String thisNodeTag;


  public OwnLocalResources(String thisNodeTag){
    this.thisNodeTag = thisNodeTag;
    LOG.info("thisNodeTag=" + thisNodeTag);
  }

  public OwnLocalResources(){
    this(getThisLocalResourceTag());
  }

  public static Set<String> getLocalResourcesAllTags(){

    Set<String> all = new HashSet<>();
    all.add(LR_TAG_CENTOS6);
    all.add(LR_TAG_CENTOS7);
    all.add(LR_TAG_MACOS10);

    return all;
  }

  public static String getThisLocalResourceTag(){
     /*
        2.6.32-431.29.2.el6.x86_64
        3.10.0-514.6.1.el7.x86_64
     */
    String osVersion = System.getProperty("os.version");
    if(osVersion == null){
      return null;
    }

    if(Shell.LINUX){

      if(osVersion.contains("el6")) {
        return LR_TAG_CENTOS6;
      }

      if(osVersion.contains("el7")) {
        return LR_TAG_CENTOS7;
      }
    }
    if(Shell.MAC){
      if(osVersion.startsWith("10.")) {
        return LR_TAG_MACOS10;
      }
    }

    return null;
  }


  public Pair<String,Path> splitTagAndBasename(Path path){

    String name = path.getName();
    Path parent = path.getParent();

    if(name.startsWith(LR_PREFIX) == false){
      return null;
    }

    name = name.substring(LR_PREFIX.length());
    int underscore = name.indexOf(LR_TAG_DELIM);

    if(underscore == -1){
      return null;
    }

    String tag = name.substring(0, underscore);
    String basename = name.substring(underscore + 1);

    if(LR_ALL_TAGS.contains(tag) == false){
      return null;
    }


    return new Pair<>(tag, new Path(parent, basename));
  }


  public Map<Path,List<String>> filterLocalResources(Map<Path,List<String>> originLocalResources){

    Map<Path,Path> linkNameMap = new HashMap<>();
    Map<Path,List<String>> filteredLRs = new HashMap<>();
    Map<Path,Path> taggingLinkNameMap = new HashMap<>();
    Map<String,Path> ignores = new HashMap<>();

    if(thisNodeTag == null){
      return new HashMap<>(originLocalResources);
    }

    for(Map.Entry<Path,List<String>> entry: originLocalResources.entrySet()) {

      Path localCachePath = entry.getKey();
      List<String> links = entry.getValue();

      for (String item : links) {

        Path link = new Path(item);
        Pair<String, Path> pair = splitTagAndBasename(link);
        if(pair == null) {

          List<String> syms = filteredLRs.get(localCachePath);

          if(syms == null) {
            syms = new ArrayList<>();
            filteredLRs.put(localCachePath, syms);
          }

          syms.add(item);
          linkNameMap.put(link, localCachePath);

        } else {

          String tag = pair.getFirst();
          Path taggingLinkName = pair.getSecond();

          if(tag.equals(thisNodeTag)) {
            taggingLinkNameMap.put(taggingLinkName, localCachePath);
          } else {
            ignores.put(item, localCachePath);
          }
        }
      }
    }


    for(Map.Entry<Path, Path> entry: taggingLinkNameMap.entrySet()){

      Path linkName = entry.getKey();
      Path thisCachePath = entry.getValue();
      List<String> syms;

      Path thatCachePath = linkNameMap.get(linkName);
      if(thatCachePath != null){
        syms = filteredLRs.remove(thatCachePath);

        for(String sym:syms){
          ignores.put(sym, thatCachePath);
        }

      } else {
        syms = new ArrayList<>();
        for(String sym: originLocalResources.get(thisCachePath)){

          Pair<String,Path> pair = splitTagAndBasename(new Path(sym));
          if(pair == null){
            syms.add(sym);
          } else {

            String tag = pair.getFirst();
            Path taggingLinkName = pair.getSecond();

            if(tag.equals(thisNodeTag)) {
              syms.add(taggingLinkName.toString());
            } else {
              ignores.put(sym, thisCachePath);
            }
          }
        }
      }

      filteredLRs.put(thisCachePath, syms);
    }

    if(ignores.size() > 0 || taggingLinkNameMap.size() > 0) {
      LOG.info("Ignored LRs:" + ignores + " Node Own LRs:" + taggingLinkNameMap);
    }

    return filteredLRs;
  }

}

