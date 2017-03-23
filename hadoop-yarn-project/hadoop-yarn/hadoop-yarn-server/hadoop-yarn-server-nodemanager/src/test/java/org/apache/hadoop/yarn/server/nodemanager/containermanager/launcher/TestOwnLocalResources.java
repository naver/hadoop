package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;


import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * OwnLocalResources Tester.
 *
 * @author <Authors name>
 * @since <pre>3월 23, 2017</pre>
 * @version 1.0
 */
public class TestOwnLocalResources {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   *
   * Method: getLocalResourcesAllTags()
   *
   */
  @Test
  public void testGetLocalResourcesAllTags() throws Exception {

    Set<String> all = OwnLocalResources.getLocalResourcesAllTags();
    assertTrue(all.contains("centos6"));
    assertTrue(all.contains("centos7"));
    assertTrue(all.contains("macos10"));

  }

  /**
   *
   * Method: splitTagAndBasename(Path path)
   *
   */
  @Test
  public void testSplitTagAndBasename() throws Exception {

    OwnLocalResources olr = new OwnLocalResources();

    Pair<String,Path> pair = olr.splitTagAndBasename(new Path("hello/_native_centos7_liba.so"));

    assertEquals("centos7", pair.getFirst());
    assertEquals("hello/liba.so", pair.getSecond().toString());


    pair = olr.splitTagAndBasename(new Path("hello/_native_centos6_liba.so"));
    assertEquals("centos6", pair.getFirst());
    assertEquals("hello/liba.so", pair.getSecond().toString());


    pair = olr.splitTagAndBasename(new Path("a/b/c/d/_native_centos6_liba.so"));
    assertEquals("centos6", pair.getFirst());
    assertEquals("a/b/c/d/liba.so", pair.getSecond().toString());


    pair = olr.splitTagAndBasename(new Path("_native_centos6_liba.so"));
    assertEquals("centos6", pair.getFirst());
    assertEquals("liba.so", pair.getSecond().toString());

    pair = olr.splitTagAndBasename(new Path("_centos6_liba.so"));
    assertEquals(null, pair);

    pair = olr.splitTagAndBasename(new Path("_native_liba.so"));
    assertEquals(null, pair);

    pair = olr.splitTagAndBasename(new Path("_native_CENTOS7_liba.so"));
    assertEquals(null, pair);

  }

  /**
   *
   * Method: filterLocalResources(Map<Path,List<String>> originLocalResources)
   *
   */
  @Test
  public void testFilterLocalResources() throws Exception {


    Map<Path,List<String>> localResources = new HashMap<>();

    Path path1 = new Path("/data1/liba.so");
    Path path2 = new Path("/data1/liba_centos7.so");
    List<String> syms1 = Arrays.asList("liba.so");
    List<String> syms2 = Arrays.asList("_native_centos7_liba.so");

    localResources.put(path1, syms1);
    localResources.put(path2, syms2);

    OwnLocalResources centos7_olr = new OwnLocalResources("centos7");


    Map<Path, List<String>> fLRs = centos7_olr.filterLocalResources(localResources);
    assertEquals(syms1, fLRs.get(path2));
    assertEquals(null, fLRs.get(path1));


    OwnLocalResources centos6_olr = new OwnLocalResources("centos6");
    fLRs = centos6_olr.filterLocalResources(localResources);
    assertEquals(null, fLRs.get(path2));
    assertEquals(syms1, fLRs.get(path1));

  }


  @Test
  public void testFilterLocalResources2() throws Exception {

    Map<Path,List<String>> localResources = new HashMap<>();

    Path path1 = new Path("/data1/liba.so");
    Path path2 = new Path("/data1/makecoll");
    List<String> syms1 = Arrays.asList("liba.so");
    List<String> syms2 = Arrays.asList("makecoll");

    localResources.put(path1, syms1);
    localResources.put(path2, syms2);

    OwnLocalResources centos7_olr = new OwnLocalResources("centos7");

    Map<Path, List<String>> fLRs = centos7_olr.filterLocalResources(localResources);
    assertEquals(syms1, fLRs.get(path1));
    assertEquals(syms2, fLRs.get(path2));

    System.out.println(fLRs);


    OwnLocalResources centos6_olr = new OwnLocalResources("centos6");
    fLRs = centos6_olr.filterLocalResources(localResources);
    assertEquals(syms1, fLRs.get(path1));
    assertEquals(syms2, fLRs.get(path2));

    System.out.println(fLRs);
  }


  @Test
  public void testFilterLocalResources3() throws Exception {

    Map<Path,List<String>> localResources = new HashMap<>();

    Path path1 = new Path("/data1/liba_centos6.so");
    Path path2 = new Path("/data1/liba_centos7.so");
    List<String> syms1 = Arrays.asList("lib/_native_centos6_liba.so");
    List<String> syms2 = Arrays.asList("lib/_native_centos7_liba.so");

    localResources.put(path1, syms1);
    localResources.put(path2, syms2);

    OwnLocalResources centos7_olr = new OwnLocalResources("centos7");

    Map<Path, List<String>> fLRs = centos7_olr.filterLocalResources(localResources);
    assertEquals(null, fLRs.get(path1));
    assertEquals("lib/liba.so", fLRs.get(path2).get(0));

    System.out.println(fLRs);


    OwnLocalResources centos6_olr = new OwnLocalResources("centos6");
    fLRs = centos6_olr.filterLocalResources(localResources);
    assertEquals("lib/liba.so", fLRs.get(path1).get(0));
    assertEquals(null, fLRs.get(path2));

    System.out.println(fLRs);
  }


  @Test
  public void testFilterLocalResources4() throws Exception {

    Map<Path,List<String>> localResources = new HashMap<>();

    Path path1 = new Path("/data1/liba_centos6.so");
    Path path2 = new Path("/data1/liba_centos7.so");
    List<String> syms1 = Arrays.asList("lib/_native_centos6_liba.so", "centos6.data");
    List<String> syms2 = Arrays.asList("lib/_native_centos7_liba.so", "centos7.data");

    localResources.put(path1, syms1);
    localResources.put(path2, syms2);

    OwnLocalResources centos7_olr = new OwnLocalResources("centos7");

    Map<Path, List<String>> fLRs = centos7_olr.filterLocalResources(localResources);
    assertEquals("centos6.data", fLRs.get(path1).get(0));
    assertEquals("lib/liba.so", fLRs.get(path2).get(0));
    assertEquals("centos7.data", fLRs.get(path2).get(1));

    System.out.println(fLRs);


    OwnLocalResources centos6_olr = new OwnLocalResources("centos6");
    fLRs = centos6_olr.filterLocalResources(localResources);
    assertEquals("lib/liba.so", fLRs.get(path1).get(0));
    assertEquals("centos6.data", fLRs.get(path1).get(1));
    assertEquals("centos7.data", fLRs.get(path2).get(0));

    System.out.println(fLRs);
  }

  @Test
  public void testFilterLocalResources5() throws Exception {

    Map<Path,List<String>> localResources = new HashMap<>();

    Path path1 = new Path("/data1/liba_centos6.so");
    Path path2 = new Path("/data1/liba_centos7.so");
    List<String> syms1 = Arrays.asList("lib/_native_centos6_liba.so", "_native_centos7_libb.so");
    List<String> syms2 = Arrays.asList("lib/_native_centos7_liba.so", "_native_centos6_libb.so");

    localResources.put(path1, syms1);
    localResources.put(path2, syms2);

    OwnLocalResources centos7_olr = new OwnLocalResources("centos7");

    Map<Path, List<String>> fLRs = centos7_olr.filterLocalResources(localResources);
    /*
    assertEquals("centos6.data", fLRs.get(path1).get(0));
    assertEquals("lib/liba.so", fLRs.get(path2).get(0));
    assertEquals("centos7.data", fLRs.get(path2).get(1));
    */

    System.out.println(fLRs);


    OwnLocalResources centos6_olr = new OwnLocalResources("centos6");
    fLRs = centos6_olr.filterLocalResources(localResources);
    /*
    assertEquals("lib/liba.so", fLRs.get(path1).get(0));
    assertEquals("centos6.data", fLRs.get(path1).get(1));
    assertEquals("centos7.data", fLRs.get(path2).get(0));
    */

    System.out.println(fLRs);
  }


}
