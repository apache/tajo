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
import java.util.Map;
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

public class TestRuleEngine {
  
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
  public void testLoadPredefinedRules() throws Exception {
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.loadPredefinedRules();
    
    assertThat(ruleEngine.getRules().size() > 0, is(true));
  }
  
  public static class TestRuleProvider1 implements SelfDiagnosisRuleProvider {

    @Override
    public List<SelfDiagnosisRule> getDefinedRules() {
      List<SelfDiagnosisRule> ruleList = new ArrayList<>();
      ruleList.add(new TestRule1());
      ruleList.add(new TestRule2());
      return ruleList;
    }
    
  }
  
  @SelfDiagnosisRuleDefinition(category="test",name = "TestRule1")
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
  
  @SelfDiagnosisRuleDefinition(category="test",name = "TestRule2")
  @SelfDiagnosisRuleVisibility.LimitedPrivate(acceptedCallers = { TestRuleEngine.class })
  public static class TestRule2 implements SelfDiagnosisRule {

    @Override
    public EvaluationResult evaluate(EvaluationContext context) {
      EvaluationResult result = new EvaluationResult();
      result.setReturnCode(EvaluationResultCode.OK);
      result.setMessage("TestRule2 has passed.");
      return result;
    }
    
  }
  
  protected Path createJarPathForTestRuleProvider1() throws Exception {
    Path jarPath = new Path(testPath, "test-jar1.jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(new File(jarPath.toUri())), manifest);
    
    JarEntry entry = new JarEntry("META-INF/services/"+SelfDiagnosisRuleProvider.class.getName());
    jarOut.putNextEntry(entry);
    jarOut.write(TestRuleProvider1.class.getName().getBytes());
    jarOut.closeEntry();
    jarOut.close();
    
    return jarPath;
  }
  
  @Test
  public void testRuleForTestRuleProvider() throws Exception {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl;
    
    cl = new URLClassLoader(new URL[] {createJarPathForTestRuleProvider1().toUri().toURL()}, parent);
    Thread.currentThread().setContextClassLoader(cl);
    
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    ruleEngine.loadPredefinedRules();
    
    Map<String, Map<String, RuleWrapper>> wrapperMap = ruleEngine.getRules();
    Map<String, RuleWrapper> testMap = wrapperMap.get("test");
    assertThat(testMap, is(notNullValue()));
    assertThat(testMap.size(), is(2));
    for (String ruleName: testMap.keySet()) {
      assertThat(ruleName, anyOf(is("TestRule1"), is("TestRule2")));
    }
    
    try {
      Method closeMethod = URLClassLoader.class.getMethod("close");
      closeMethod.invoke(cl);
    } catch (NoSuchMethodException ignored) {
    }
  }
  
}
