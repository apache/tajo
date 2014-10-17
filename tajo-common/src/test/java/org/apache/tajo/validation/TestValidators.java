package org.apache.tajo.validation;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import org.apache.tajo.util.TUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

public class TestValidators {
  
  private class ValidatorClazzMatcher<T extends Validator> extends org.hamcrest.TypeSafeDiagnosingMatcher<ConstraintViolation> {
    
    private final org.hamcrest.Matcher<Class<T>> matcher;
    
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
  
  private <T extends Validator> org.hamcrest.Matcher<? super ConstraintViolation> 
      hasAClass(Matcher<Class<T>> matcher) {
    return new ValidatorClazzMatcher<T>(matcher);
  }
  
  @Test
  public void testNotNullValidator() {
    Object testValue = null;
    
    assertThat(new NotNullValidator().validateInternal(testValue), is(false));
    assertThat(new NotNullValidator().validate(testValue).size(), is(1));
    assertThat(new NotNullValidator().validate(testValue),
        hasItem(new ValidatorClazzMatcher<NotNullValidator>(equalTo(NotNullValidator.class))));
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
    
    validators = TUtil.newHashSet();
    validators.add(new PatternValidator("^[a-zA-Z]+://"));
    validators.add(new LengthValidator(255));
    assertThat(new GroupValidator(validators).validate(httpUrl).size(), is(0));
    assertThat(new GroupValidator(validators).validate("tajo").size(), is(2));
    assertThat(new GroupValidator(validators).validate("tajo"),
        hasItem(hasAClass(equalTo(PatternValidator.class))));
    
    validators = TUtil.newHashSet();
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
    
    validators = TUtil.newHashSet();
    validators.add(new PatternValidator("^[a-zA-Z]+://"));
    validators.add(new LengthValidator(255));
    new GroupValidator(validators).validate("tajo", true);
    
    fail();
  }

}
