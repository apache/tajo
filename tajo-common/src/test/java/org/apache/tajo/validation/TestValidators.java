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

package org.apache.tajo.validation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.*;

import org.apache.tajo.util.TUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

public class TestValidators {
  
  private class ValidatorClazzMatcher<T extends Validator> extends TypeSafeDiagnosingMatcher<ConstraintViolation> {
    
    private final Matcher<Class<T>> matcher;
    
    public ValidatorClazzMatcher(Matcher<Class<T>> matcher) {
      this.matcher = matcher;
    }

    @Override
    public void describeTo(Description description) {
      description
      .appendText("validatorClazz property of ConstraintViolation class containing ")
      .appendDescriptionOf(matcher);
    }

    @Override
    protected boolean matchesSafely(ConstraintViolation item, Description mismatchDescription) {
      if (matcher.matches(item.getValidatorClazz())) {
        return true;
      }
      matcher.describeMismatch(item, mismatchDescription);
      return false;
    }
    
  }
  
  private class CollectionMatcher<T> extends TypeSafeDiagnosingMatcher<Iterable<? extends T>> {
    
    private final Matcher<? extends T> matcher;
    
    public CollectionMatcher(Matcher<? extends T> matcher) {
      this.matcher = matcher;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a collection containing ").appendDescriptionOf(this.matcher);
    }

    @Override
    protected boolean matchesSafely(Iterable<? extends T> item, Description mismatchDescription) {
      boolean isFirst = true;

      for (T obj : item) {
        if (this.matcher.matches(obj)) {
          return true;
        }

        if (!isFirst) {
          mismatchDescription.appendText(", ");
        }

        this.matcher.describeMismatch(obj, mismatchDescription);
        isFirst = false;
      }
      return false;
    }
    
  }
  
  private <T extends Validator> Matcher<? super ConstraintViolation> hasAClass(Matcher<Class<T>> matcher) {
    return new ValidatorClazzMatcher<>(matcher);
  }
  
  private <T> Matcher<Iterable<? extends T>> hasItem(Matcher<? extends T> matcher) {
    return new CollectionMatcher<>(matcher);
  }
  
  @Test
  public void testNotNullValidator() {
    Object testValue = null;
    
    assertThat(new NotNullValidator().validateInternal(testValue), is(false));
    assertThat(new NotNullValidator().validate(testValue).size(), is(1));
    assertThat(new NotNullValidator().validate(testValue),
        hasItem(hasAClass(equalTo(NotNullValidator.class))));
  }

  @Test
  public void testMinValidator() {
    byte byteValue;
    short shortValue;
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    BigInteger bigIntegerValue;
    BigDecimal bigDecimalValue;
    
    byteValue = 2;
    assertThat(new MinValidator(Byte.toString(Byte.MIN_VALUE)).validateInternal(byteValue), is(true));
    assertThat(new MinValidator(Byte.toString(Byte.MIN_VALUE)).validate(byteValue).size(), is(0));
    assertThat(new MinValidator(Byte.toString(Byte.MAX_VALUE)).validateInternal(byteValue), is(false));
    assertThat(new MinValidator(Byte.toString(Byte.MAX_VALUE)).validate(byteValue).size(), is(1));
    assertThat(new MinValidator(Byte.toString(Byte.MAX_VALUE)).validate(byteValue), 
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    shortValue = 3;
    assertThat(new MinValidator(Short.toString(Short.MIN_VALUE)).validateInternal(shortValue), is(true));
    assertThat(new MinValidator(Short.toString(Short.MIN_VALUE)).validate(shortValue).size(), is(0));
    assertThat(new MinValidator(Short.toString(Short.MAX_VALUE)).validateInternal(shortValue), is(false));
    assertThat(new MinValidator(Short.toString(Short.MAX_VALUE)).validate(shortValue).size(), is(1));
    assertThat(new MinValidator(Short.toString(Short.MAX_VALUE)).validate(shortValue),
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    intValue = 4;
    assertThat(new MinValidator(Integer.toString(Integer.MIN_VALUE)).validateInternal(intValue), is(true));
    assertThat(new MinValidator(Integer.toString(Integer.MIN_VALUE)).validate(intValue).size(), is(0));
    assertThat(new MinValidator(Integer.toString(Integer.MAX_VALUE)).validateInternal(intValue), is(false));
    assertThat(new MinValidator(Integer.toString(Integer.MAX_VALUE)).validate(intValue).size(), is(1));
    assertThat(new MinValidator(Integer.toString(Integer.MAX_VALUE)).validate(intValue),
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    longValue = 5;
    assertThat(new MinValidator(Long.toString(Long.MIN_VALUE)).validateInternal(longValue), is(true));
    assertThat(new MinValidator(Long.toString(Long.MIN_VALUE)).validate(longValue).size(), is(0));
    assertThat(new MinValidator(Long.toString(Long.MAX_VALUE)).validateInternal(longValue), is(false));
    assertThat(new MinValidator(Long.toString(Long.MAX_VALUE)).validate(longValue).size(), is(1));
    assertThat(new MinValidator(Long.toString(Long.MAX_VALUE)).validate(longValue),
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    floatValue = 4.7f;
    assertThat(new MinValidator(Float.toString(Float.MIN_VALUE)).validateInternal(floatValue), is(true));
    assertThat(new MinValidator(Float.toString(Float.MIN_VALUE)).validate(floatValue).size(), is(0));
    assertThat(new MinValidator(Float.toString(Float.MAX_VALUE)).validateInternal(floatValue), is(false));
    assertThat(new MinValidator(Float.toString(Float.MAX_VALUE)).validate(floatValue).size(), is(1));
    assertThat(new MinValidator(Float.toString(Float.MAX_VALUE)).validate(floatValue),
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    doubleValue = 7.5e10;
    assertThat(new MinValidator(Double.toString(Double.MIN_VALUE)).validateInternal(doubleValue), is(true));
    assertThat(new MinValidator(Double.toString(Double.MIN_VALUE)).validate(doubleValue).size(), is(0));
    assertThat(new MinValidator(Double.toString(Double.MAX_VALUE)).validateInternal(doubleValue), is(false));
    assertThat(new MinValidator(Double.toString(Double.MAX_VALUE)).validate(doubleValue).size(), is(1));
    assertThat(new MinValidator(Double.toString(Double.MAX_VALUE)).validate(doubleValue),
        hasItem(hasAClass(equalTo(MinValidator.class))));
    
    bigIntegerValue = new BigInteger(10, new Random());
    assertThat(new MinValidator(new BigInteger(Long.toString(Long.MIN_VALUE)).toString(10))
      .validateInternal(bigIntegerValue), is(true));
    assertThat(new MinValidator(new BigInteger(Long.toString(Long.MIN_VALUE)).toString(10))
      .validate(bigIntegerValue).size(), is(0));
    assertThat(new MinValidator(new BigInteger(Long.toString(Long.MAX_VALUE)).toString(10))
      .validateInternal(bigIntegerValue), is(false));
    assertThat(new MinValidator(new BigInteger(Long.toString(Long.MAX_VALUE)).toString(10))
      .validate(bigIntegerValue).size(), is(1));
    assertThat(new MinValidator(new BigInteger(Long.toString(Long.MAX_VALUE)).toString(10))
      .validate(bigIntegerValue), hasItem(hasAClass(equalTo(MinValidator.class))));
    
    bigDecimalValue = new BigDecimal(new BigInteger(10, new Random()), MathContext.DECIMAL64);
    assertThat(new MinValidator(new BigDecimal(Double.MIN_VALUE).toString())
      .validateInternal(bigDecimalValue), is(true));
    assertThat(new MinValidator(new BigDecimal(Double.MIN_VALUE).toString())
      .validate(bigDecimalValue).size(), is(0));
    assertThat(new MinValidator(new BigDecimal(Double.MAX_VALUE).toString())
      .validateInternal(bigDecimalValue), is(false));
    assertThat(new MinValidator(new BigDecimal(Double.MAX_VALUE).toString())
      .validate(bigDecimalValue).size(), is(1));
    assertThat(new MinValidator(new BigDecimal(Double.MAX_VALUE).toString())
      .validate(bigDecimalValue), hasItem(hasAClass(equalTo(MinValidator.class))));
  }
  
  @Test
  public void testMaxValidator() {
    byte byteValue;
    short shortValue;
    int intValue;
    long longValue;
    float floatValue;
    double doubleValue;
    BigInteger bigIntegerValue;
    BigDecimal bigDecimalValue;
    
    byteValue = 2;
    assertThat(new MaxValidator(Byte.toString(Byte.MAX_VALUE)).validateInternal(byteValue), is(true));
    assertThat(new MaxValidator(Byte.toString(Byte.MAX_VALUE)).validate(byteValue).size(), is(0));
    assertThat(new MaxValidator(Byte.toString(Byte.MIN_VALUE)).validateInternal(byteValue), is(false));
    assertThat(new MaxValidator(Byte.toString(Byte.MIN_VALUE)).validate(byteValue).size(), is(1));
    assertThat(new MaxValidator(Byte.toString(Byte.MIN_VALUE)).validate(byteValue), 
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    shortValue = 3;
    assertThat(new MaxValidator(Short.toString(Short.MAX_VALUE)).validateInternal(shortValue), is(true));
    assertThat(new MaxValidator(Short.toString(Short.MAX_VALUE)).validate(shortValue).size(), is(0));
    assertThat(new MaxValidator(Short.toString(Short.MIN_VALUE)).validateInternal(shortValue), is(false));
    assertThat(new MaxValidator(Short.toString(Short.MIN_VALUE)).validate(shortValue).size(), is(1));
    assertThat(new MaxValidator(Short.toString(Short.MIN_VALUE)).validate(shortValue),
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    intValue = 4;
    assertThat(new MaxValidator(Integer.toString(Integer.MAX_VALUE)).validateInternal(intValue), is(true));
    assertThat(new MaxValidator(Integer.toString(Integer.MAX_VALUE)).validate(intValue).size(), is(0));
    assertThat(new MaxValidator(Integer.toString(Integer.MIN_VALUE)).validateInternal(intValue), is(false));
    assertThat(new MaxValidator(Integer.toString(Integer.MIN_VALUE)).validate(intValue).size(), is(1));
    assertThat(new MaxValidator(Integer.toString(Integer.MIN_VALUE)).validate(intValue),
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    longValue = 5;
    assertThat(new MaxValidator(Long.toString(Long.MAX_VALUE)).validateInternal(longValue), is(true));
    assertThat(new MaxValidator(Long.toString(Long.MAX_VALUE)).validate(longValue).size(), is(0));
    assertThat(new MaxValidator(Long.toString(Long.MIN_VALUE)).validateInternal(longValue), is(false));
    assertThat(new MaxValidator(Long.toString(Long.MIN_VALUE)).validate(longValue).size(), is(1));
    assertThat(new MaxValidator(Long.toString(Long.MIN_VALUE)).validate(longValue),
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    floatValue = 4.7f;
    assertThat(new MaxValidator(Float.toString(Float.MAX_VALUE)).validateInternal(floatValue), is(true));
    assertThat(new MaxValidator(Float.toString(Float.MAX_VALUE)).validate(floatValue).size(), is(0));
    assertThat(new MaxValidator(Float.toString(Float.MIN_VALUE)).validateInternal(floatValue), is(false));
    assertThat(new MaxValidator(Float.toString(Float.MIN_VALUE)).validate(floatValue).size(), is(1));
    assertThat(new MaxValidator(Float.toString(Float.MIN_VALUE)).validate(floatValue),
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    doubleValue = 7.5e10;
    assertThat(new MaxValidator(Double.toString(Double.MAX_VALUE)).validateInternal(doubleValue), is(true));
    assertThat(new MaxValidator(Double.toString(Double.MAX_VALUE)).validate(doubleValue).size(), is(0));
    assertThat(new MaxValidator(Double.toString(Double.MIN_VALUE)).validateInternal(doubleValue), is(false));
    assertThat(new MaxValidator(Double.toString(Double.MIN_VALUE)).validate(doubleValue).size(), is(1));
    assertThat(new MaxValidator(Double.toString(Double.MIN_VALUE)).validate(doubleValue),
        hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    bigIntegerValue = new BigInteger(10, new Random());
    assertThat(new MaxValidator(new BigInteger(Long.toString(Long.MAX_VALUE)).toString(10))
      .validateInternal(bigIntegerValue), is(true));
    assertThat(new MaxValidator(new BigInteger(Long.toString(Long.MAX_VALUE)).toString(10))
      .validate(bigIntegerValue).size(), is(0));
    assertThat(new MaxValidator(new BigInteger(Long.toString(Long.MIN_VALUE)).toString(10))
      .validateInternal(bigIntegerValue), is(false));
    assertThat(new MaxValidator(new BigInteger(Long.toString(Long.MIN_VALUE)).toString(10))
      .validate(bigIntegerValue).size(), is(1));
    assertThat(new MaxValidator(new BigInteger(Long.toString(Long.MIN_VALUE)).toString(10))
      .validate(bigIntegerValue), hasItem(hasAClass(equalTo(MaxValidator.class))));
    
    bigDecimalValue = new BigDecimal(new BigInteger(10, new Random()), MathContext.DECIMAL64);
    assertThat(new MaxValidator(new BigDecimal(Double.MAX_VALUE).toString())
      .validateInternal(bigDecimalValue), is(true));
    assertThat(new MaxValidator(new BigDecimal(Double.MAX_VALUE).toString())
      .validate(bigDecimalValue).size(), is(0));
    assertThat(new MaxValidator(new BigDecimal(Double.MIN_VALUE).toString())
      .validateInternal(bigDecimalValue), is(false));
    assertThat(new MaxValidator(new BigDecimal(Double.MIN_VALUE).toString())
      .validate(bigDecimalValue).size(), is(1));
    assertThat(new MaxValidator(new BigDecimal(Double.MIN_VALUE).toString())
      .validate(bigDecimalValue), hasItem(hasAClass(equalTo(MaxValidator.class))));
  }
  
  @Test
  public void testPatternValidator() {
    String schemeString = "http://tajo.apache.org";
    assertThat(new PatternValidator("^([a-zA-Z])+://").validateInternal(schemeString), is(true));
    assertThat(new PatternValidator("^([a-zA-Z])+://").validate(schemeString).size(), is(0));
    assertThat(new PatternValidator("([a-zA-Z])+://$").validateInternal(schemeString), is(false));
    assertThat(new PatternValidator("([a-zA-Z])+://$").validate(schemeString).size(), is(1));
    assertThat(new PatternValidator("([a-zA-Z])+://$").validate(schemeString),
        hasItem(hasAClass(equalTo(PatternValidator.class))));
  }
  
  @Test
  public void testLengthValidator() {
    String shortString = "12345";
    String longString = UUID.randomUUID().toString();
    
    assertThat(new LengthValidator(10).validateInternal(shortString), is(true));
    assertThat(new LengthValidator(10).validate(shortString).size(), is(0));
    assertThat(new LengthValidator(3).validateInternal(shortString), is(false));
    assertThat(new LengthValidator(3).validate(shortString).size(), is(1));
    assertThat(new LengthValidator(3).validate(shortString), 
        hasItem(hasAClass(equalTo(LengthValidator.class))));
    
    assertThat(new LengthValidator(40).validateInternal(longString), is(true));
    assertThat(new LengthValidator(40).validate(longString).size(), is(0));
    assertThat(new LengthValidator(10).validateInternal(longString), is(false));
    assertThat(new LengthValidator(10).validate(longString).size(), is(1));
    assertThat(new LengthValidator(10).validate(longString), 
        hasItem(hasAClass(equalTo(LengthValidator.class))));
  }
  
  @Test
  public void testGroupValidator() {
    String httpUrl = "http://tajo.apache.org";
    Collection<Validator> validators = null;
    
    validators = new HashSet<>();
    validators.add(new PatternValidator("^[a-zA-Z]+://"));
    validators.add(new LengthValidator(255));
    assertThat(new GroupValidator(validators).validate(httpUrl).size(), is(0));
    assertThat(new GroupValidator(validators).validate("tajo").size(), is(1));
    assertThat(new GroupValidator(validators).validate("tajo"),
        hasItem(hasAClass(equalTo(PatternValidator.class))));
    
    validators = new HashSet<>();
    validators.add(new PatternValidator("^[a-zA-Z]+://"));
    validators.add(new LengthValidator(7));
    assertThat(new GroupValidator(validators).validate(httpUrl).size(), is(1));
    assertThat(new GroupValidator(validators).validate(httpUrl),
        hasItem(hasAClass(equalTo(LengthValidator.class))));
  }
  
  @Test
  public void testRangeValidator() {
    int intValue = 5;
    
    assertThat(new RangeValidator("0", Integer.toString(Integer.MAX_VALUE)).validateInternal(intValue), 
        is(true));
    assertThat(new RangeValidator("1024", "2048").validateInternal(intValue), is(false));
  }
  
  @Test(expected=ConstraintViolationException.class)
  public void testExceptionThrow() {
    Collection<Validator> validators = null;
    
    validators = new HashSet<>();
    validators.add(new PatternValidator("^[a-zA-Z]+://"));
    validators.add(new LengthValidator(255));
    new GroupValidator(validators).validate("tajo", true);
    
    fail();
  }
  
  @Test
  public void testPathValidator() {
    String validUrl = "file:///tmp/tajo-$root/";    
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    validUrl = "file:///tmp/tajo-${user.name}/";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    validUrl = "file:/home/tajo/test-data/TestExternalSortExec";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    validUrl = "file:///C:/Windows/System32";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    validUrl = "/C:/Windows/system32/driver";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    validUrl = "/tmp/tajo-root/";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));

    validUrl = "hdfs://localhost:7020/tajo-root";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));

    validUrl = "hdfs://127.0.0.1:7020/tajo-root";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));

    validUrl = "hdfs://192.168.0.1:7020/tajo-root";
    assertThat(new PathValidator().validateInternal(validUrl), is(true));
    assertThat(new PathValidator().validate(validUrl).size(), is(0));
    
    String invalidUrl = "t!ef:///tmp/tajo-root";
    assertThat(new PathValidator().validateInternal(invalidUrl), is(false));
    assertThat(new PathValidator().validate(invalidUrl).size(), is(1));
    assertThat(new PathValidator().validate(invalidUrl),
        hasItem(hasAClass(equalTo(PathValidator.class))));
    
    invalidUrl = "This is not a valid url.";
    assertThat(new PathValidator().validateInternal(invalidUrl), is(false));
    assertThat(new PathValidator().validate(invalidUrl).size(), is(1));
    assertThat(new PathValidator().validate(invalidUrl),
        hasItem(hasAClass(equalTo(PathValidator.class))));
  }

  @Test
  public void testPathListValidator() {
    String validUrls =
        "file:///tmp/tajo-$root/a,file:///tmp/tajo-${user.name}/,file:/home/tajo/test-data/TestExternalSortExec";
    assertThat(new PathListValidator().validateInternal(validUrls), is(true));
    assertThat(new PathListValidator().validate(validUrls).size(), is(0));

    validUrls =
        "file:///tmp/tajo-$root/a,  file:///tmp/tajo-${user.name}/, file:/home/tajo/test-data/TestExternalSortExec";
    assertThat(new PathListValidator().validateInternal(validUrls), is(true));
    assertThat(new PathListValidator().validate(validUrls).size(), is(0));

    validUrls = "/tmp/tajo-hyunsik/tmpdir1,/tmp/tajo-hyunsik/tmpdir2";
    assertThat(new PathListValidator().validateInternal(validUrls), is(true));
    assertThat(new PathListValidator().validate(validUrls).size(), is(0));

    String invalidUrls =
        "file:///tmp/tajo-$root/a,  t!ef:///tmp/tajo-root, file:/home/tajo/test-data/TestExternalSortExec";
    assertThat(new PathListValidator().validateInternal(invalidUrls), is(false));
    assertThat(new PathListValidator().validate(invalidUrls).size(), is(1));
    assertThat(new PathListValidator().validate(invalidUrls),
        hasItem(hasAClass(equalTo(PathListValidator.class))));

    invalidUrls = "This is not a valid url,This is not a valid url";
    assertThat(new PathListValidator().validateInternal(invalidUrls), is(false));
    assertThat(new PathListValidator().validate(invalidUrls).size(), is(1));
    assertThat(new PathListValidator().validate(invalidUrls),
        hasItem(hasAClass(equalTo(PathListValidator.class))));
  }
  
  @Test
  public void testShellVariableValidator() {
    String validVariable = "${user.name}";
    assertThat(new ShellVariableValidator().validateInternal(validVariable), is(true));
    assertThat(new ShellVariableValidator().validate(validVariable).size(), is(0));
    
    validVariable = "$SHELL";
    assertThat(new ShellVariableValidator().validateInternal(validVariable), is(true));
    assertThat(new ShellVariableValidator().validate(validVariable).size(), is(0));
    
    validVariable = "STRING";
    assertThat(new ShellVariableValidator().validateInternal(validVariable), is(true));
    assertThat(new ShellVariableValidator().validate(validVariable).size(), is(0));
    
    String invalidVariable = "Invalid Shell Variable Name";
    assertThat(new ShellVariableValidator().validateInternal(invalidVariable), is(false));
    assertThat(new ShellVariableValidator().validate(invalidVariable).size(), is(1));
    assertThat(new ShellVariableValidator().validate(invalidVariable),
        hasItem(hasAClass(equalTo(ShellVariableValidator.class))));
  }
  
  @Test
  public void testNetworkAddressValidator() {
    String validNetworkAddress = "localhost:5000";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "192.168.0.1:5000";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "0.0.0.0:28094";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "Tajo-Test.apache.org";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "192.168.122.1";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "[2001:db8::ff00:42:8329]:20089";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "2001:db8::ff00:42:8330:20089";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    validNetworkAddress = "2001:db8::ff00:42:8331.20089";
    assertThat(new NetworkAddressValidator().validateInternal(validNetworkAddress), is(true));
    assertThat(new NetworkAddressValidator().validate(validNetworkAddress).size(), is(0));
    
    String invalidNetAddr = "5000";
    assertThat(new NetworkAddressValidator().validateInternal(invalidNetAddr), is(false));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr).size(), is(1));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr),
        hasItem(hasAClass(equalTo(NetworkAddressValidator.class))));
    
    invalidNetAddr = "192.168.:";
    assertThat(new NetworkAddressValidator().validateInternal(invalidNetAddr), is(false));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr).size(), is(1));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr),
        hasItem(hasAClass(equalTo(NetworkAddressValidator.class))));
    
    invalidNetAddr = "localhost:98765";
    assertThat(new NetworkAddressValidator().validateInternal(invalidNetAddr), is(false));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr).size(), is(1));
    assertThat(new NetworkAddressValidator().validate(invalidNetAddr),
        hasItem(hasAClass(equalTo(NetworkAddressValidator.class))));
  }
  
  @Test
  public void testBooleanValidator() {
    String validBoolean = "true";
    assertThat(new BooleanValidator().validateInternal(validBoolean), is(true));
    assertThat(new BooleanValidator().validate(validBoolean).size(), is(0));
    
    validBoolean = "false";
    assertThat(new BooleanValidator().validateInternal(validBoolean), is(true));
    assertThat(new BooleanValidator().validate(validBoolean).size(), is(0));
    
    assertThat(new BooleanValidator().validateInternal(true), is(true));
    assertThat(new BooleanValidator().validate(true).size(), is(0));
    
    assertThat(new BooleanValidator().validateInternal(false), is(true));
    assertThat(new BooleanValidator().validate(false).size(), is(0));
    
    String invalidBoolean = "yes";
    assertThat(new BooleanValidator().validateInternal(invalidBoolean), is(false));
    assertThat(new BooleanValidator().validate(invalidBoolean).size(), is(1));
    assertThat(new BooleanValidator().validate(invalidBoolean), 
        hasItem(hasAClass(equalTo(BooleanValidator.class))));
    
    invalidBoolean = "nope";
    assertThat(new BooleanValidator().validateInternal(invalidBoolean), is(false));
    assertThat(new BooleanValidator().validate(invalidBoolean).size(), is(1));
    assertThat(new BooleanValidator().validate(invalidBoolean), 
        hasItem(hasAClass(equalTo(BooleanValidator.class))));
    
    invalidBoolean = "invalid";
    assertThat(new BooleanValidator().validateInternal(invalidBoolean), is(false));
    assertThat(new BooleanValidator().validate(invalidBoolean).size(), is(1));
    assertThat(new BooleanValidator().validate(invalidBoolean), 
        hasItem(hasAClass(equalTo(BooleanValidator.class))));
  }
  
  @Test
  public void testClassValidator() {
    String clazzName = "org.apache.tajo.validation.ClassValidator";
    assertThat(new ClassValidator().validateInternal(clazzName), is(true));
    assertThat(new ClassValidator().validate(clazzName).size(), is(0));
    
    clazzName = "org.apache.tajo.ConfigKey";
    assertThat(new ClassValidator().validateInternal(clazzName), is(true));
    assertThat(new ClassValidator().validate(clazzName).size(), is(0));
    
    String invalidClazzName = "invalid-.class.name";
    assertThat(new ClassValidator().validateInternal(invalidClazzName), is(false));
    assertThat(new ClassValidator().validate(invalidClazzName).size(), is(1));
    assertThat(new ClassValidator().validate(invalidClazzName), 
        hasItem(hasAClass(equalTo(ClassValidator.class))));
  }
  
  @Test
  public void testStringValidator() {
    String validAsciiString = "tajo-root900";
    assertThat(new JavaStringValidator().validateInternal(validAsciiString), is(true));
    assertThat(new JavaStringValidator().validate(validAsciiString).size(), is(0));
    
    String validMultibyteString = "타조009";
    assertThat(new JavaStringValidator().validateInternal(validMultibyteString), is(true));
    assertThat(new JavaStringValidator().validate(validMultibyteString).size(), is(0));
    
    String invalidAsciiString = "   inva - ";
    assertThat(new JavaStringValidator().validateInternal(invalidAsciiString), is(false));
    assertThat(new JavaStringValidator().validate(invalidAsciiString).size(), is(1));
    assertThat(new JavaStringValidator().validate(invalidAsciiString),
        hasItem(hasAClass(equalTo(JavaStringValidator.class))));
  }

}
