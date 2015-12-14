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

package org.apache.tajo.rule;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRuleEngine.RuleWrapper;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRuleSession {
  
  private static Path testPath;
  
  @BeforeClass
  public static void setUp() throws Exception {
    testPath = CommonTestingUtil.getTestDir();
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    CommonTestingUtil.cleanupTestDir(testPath.toUri().getPath());
  }
  
  @Test
  public void testGetCallerClassName() throws Exception {
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    
    assertThat(ruleSession.getCallerClassName(), is(notNullValue()));
    assertThat(ruleSession.getCallerClassName().getName(), is(TestRuleSession.class.getName()));
  }
  
  public static class TestRuleSessionProvider implements SelfDiagnosisRuleProvider {

    @Override
    public List<SelfDiagnosisRule> getDefinedRules() {
      List<SelfDiagnosisRule> ruleList = new ArrayList<>();
      ruleList.add(new TestRule1());
      ruleList.add(new TestRule2());
      ruleList.add(new TestRule3());
      return ruleList;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test1",name = "TestRule1")
  @SelfDiagnosisRuleVisibility.Public
  public static class TestRule1 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestRule1 has passed.");
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test1",name = "TestRule2")
  @SelfDiagnosisRuleVisibility.LimitedPrivate(acceptedCallers = { TestRuleSession.class })
  public static class TestRule2 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestRule2 has passed.");
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test1",name = "TestRule3")
  @SelfDiagnosisRuleVisibility.LimitedPrivate(acceptedCallers = { TestRuleEngine.class })
  public static class TestRule3 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestRule3 has passed.");
      return result;
    }
    
  }
  
  protected Path createServiceJar(String className) throws Exception {
    Path jarPath = new Path(testPath, className+".jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(new File(jarPath.toUri())), manifest);
    
    JarEntry entry = new JarEntry("META-INF/services/"+SelfDiagnosisRuleProvider.class.getName());
    jarOut.putNextEntry(entry);
    jarOut.write(className.getBytes());
    jarOut.closeEntry();
    jarOut.close();
    
    return jarPath;
  }
  
  protected Path createJarPathForTestRuleSession() throws Exception {
    return createServiceJar(TestRuleSessionProvider.class.getName());
  }

  @Test
  public void testGetCandidateRules() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForTestRuleSession().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    List<RuleWrapper> candidateRules = ruleSession.withCategoryNames("test1").getCandidateRules();
    
    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 2, is(true));
    
    for (RuleWrapper wrapper: candidateRules) {
      assertThat(wrapper.getRuleName(), anyOf(is("TestRule1"), is("TestRule2")));
    }
    
    candidateRules = ruleSession.withRuleNames("TestRule1").getCandidateRules();
    
    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 1, is(true));
    
    for (RuleWrapper wrapper: candidateRules) {
      assertThat(wrapper.getRuleName(), is("TestRule1"));
    }
    
    try {
      Method closeMethod = URLClassLoader.class.getMethod("close");
      closeMethod.invoke(cl);
    } catch (NoSuchMethodException ignored) {
    }
  }
  
  @Test
  public void testReset() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForTestRuleSession().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    List<RuleWrapper> candidateRules = ruleSession.withCategoryNames("test1")
        .withRuleNames("TestRule1").getCandidateRules();
    
    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 1, is(true));
    for (RuleWrapper wrapper: candidateRules) {
      assertThat(wrapper.getRuleName(), is("TestRule1"));
    }
    
    candidateRules = ruleSession.reset().withCategoryNames("test1").getCandidateRules();
    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 2, is(true));
    
    for (RuleWrapper wrapper: candidateRules) {
      assertThat(wrapper.getRuleName(), anyOf(is("TestRule1"), is("TestRule2")));
    }
    
    try {
      Method closeMethod = URLClassLoader.class.getMethod("close");
      closeMethod.invoke(cl);
    } catch (NoSuchMethodException ignored) {
    }
  }
  
  public static class TestRulePriorityProvider implements SelfDiagnosisRuleProvider {

    @Override
    public List<SelfDiagnosisRule> getDefinedRules() {
      List<SelfDiagnosisRule> ruleList = new ArrayList<>();
      ruleList.add(new TestPriorityRule1());
      ruleList.add(new TestPriorityRule2());
      ruleList.add(new TestPriorityRule3());
      ruleList.add(new TestPriorityRule4());
      return ruleList;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test2",name = "TestPriorityRule1")
  @SelfDiagnosisRuleVisibility.Public
  public static class TestPriorityRule1 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestPriorityRule1 has passed.");
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test2",name = "TestPriorityRule2")
  @SelfDiagnosisRuleVisibility.Public
  public static class TestPriorityRule2 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestPriorityRule2 has passed.");
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test2",name = "TestPriorityRule3",priority=0)
  @SelfDiagnosisRuleVisibility.Public
  public static class TestPriorityRule3 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestPriorityRule3 has passed.");
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test2",name = "TestPriorityRule4",priority=10)
  @SelfDiagnosisRuleVisibility.Public
  public static class TestPriorityRule4 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestPriorityRule4 has passed.");
      return result;
    }
    
  }
  
  protected Path createJarPathForRulePriority() throws Exception {
    return createServiceJar(TestRulePriorityProvider.class.getName());
  }
  
  @Test
  public void testRulePriority() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForRulePriority().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    List<RuleWrapper> candidateRules = ruleSession.withCategoryNames("test2").getCandidateRules();

    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 4, is(true));
    assertThat(candidateRules.get(0).getRuleName(), is("TestPriorityRule3"));
    assertThat(candidateRules.get(1).getRuleName(), is("TestPriorityRule4"));
    
    candidateRules = ruleSession.withRuleNames("TestPriorityRule1", "TestPriorityRule2", "TestPriorityRule4")
        .getCandidateRules();
    
    assertThat(candidateRules, is(notNullValue()));
    assertThat(candidateRules.size() == 3, is(true));
    assertThat(candidateRules.get(0).getRuleName(), is("TestPriorityRule4"));
    
    try {
      Method closeMethod = URLClassLoader.class.getMethod("close");
      closeMethod.invoke(cl);
    } catch (NoSuchMethodException ignored) {
    }
  }
  
  public static class TestExecutionRuleProvider implements SelfDiagnosisRuleProvider {

    @Override
    public List<SelfDiagnosisRule> getDefinedRules() {
      List<SelfDiagnosisRule> ruleList = new ArrayList<>();
      ruleList.add(new TestExecRule1());
      ruleList.add(new TestExecRule2());
      ruleList.add(new TestExecRule3());
      return ruleList;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test3",name="TestExecRule1",priority=0)
  @SelfDiagnosisRuleVisibility.Public
  public static class TestExecRule1 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      Object rule1_param1 = context.getParameter("TestExecRule1_param1");
      try {
        if (rule1_param1 != null && rule1_param1 instanceof Integer) {
          int rule1_param1_intVal = Integer.parseInt(rule1_param1.toString());
          if (rule1_param1_intVal == 0) {
            result.setReturnCode(EvaluationResultCode.ERROR);
            result.setMessage("parameter1 is 0.");
          } else {
            result.setReturnCode(EvaluationResultCode.OK);
          }
        } else {
          result.setReturnCode(EvaluationResultCode.ERROR);
          result.setMessage("parameter1 is null or not a integer type.");
        }
      } catch (NumberFormatException e) {
        result.setReturnCode(EvaluationResultCode.ERROR);
        result.setMessage(e.getMessage());
        result.setThrowable(e);
      }
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test3",name="TestExecRule2",priority=1)
  @SelfDiagnosisRuleVisibility.Public
  public static class TestExecRule2 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      Object rule2_param1 = context.getParameter("TestExecRule2_param1");
      if (rule2_param1 == null) {
        result.setReturnCode(EvaluationResultCode.ERROR);
        result.setMessage("parameter1 is null.");
      } else {
        result.setReturnCode(EvaluationResultCode.OK);
      }
      return result;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test3",name="TestExecRule3",priority=2)
  @SelfDiagnosisRuleVisibility.Public
  public static class TestExecRule3 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      Object rule3_param1 = context.getParameter("TestExecRule3_param1");
      
      if (rule3_param1 != null && rule3_param1 instanceof String) {
        String rule3_param1_string = (String) rule3_param1;
        if (rule3_param1_string.startsWith("test")) {
          result.setReturnCode(EvaluationResultCode.OK);
        } else {
          result.setReturnCode(EvaluationResultCode.ERROR);
          result.setMessage("parameter1 does not start with 'test'.");
        }
      } else {
        result.setReturnCode(EvaluationResultCode.ERROR);
        result.setMessage("parameter1 is null or not a string type.");
      }
      return result;
    }
    
  }
  
  protected Path createJarPathForExecutingRules() throws Exception {
    return createServiceJar(TestExecutionRuleProvider.class.getName());
  }
  
  @Test
  public void testFireRules() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForExecutingRules().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    EvaluationContext context = new EvaluationContext();
    
    context.addParameter("TestExecRule1_param1", (int)5);
    context.addParameter("TestExecRule2_param1", "rule2");
    context.addParameter("TestExecRule3_param1", "testResult");
    
    ruleSession.withCategoryNames("test3").fireRules(context);
    
    try {
      Method closeMethod = URLClassLoader.class.getMethod("close");
      closeMethod.invoke(cl);
    } catch (NoSuchMethodException ignored) {
    }
  }
  
  @Test(expected=EvaluationFailedException.class)
  public void testFireRulesWithException() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForExecutingRules().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.reset();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    EvaluationContext context = new EvaluationContext();
    
    context.addParameter("TestExecRule1_param1", (int)0);
    context.addParameter("TestExecRule2_param1", "rule2");
    context.addParameter("TestExecRule3_param1", "testResult");
    
    try {
      ruleSession.withCategoryNames("test3").fireRules(context);
    } finally {
      try {
        Method closeMethod = URLClassLoader.class.getMethod("close");
        closeMethod.invoke(cl);
      } catch (NoSuchMethodException ignored) {
      }
    }
  }
}
