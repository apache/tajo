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

package org.apache.tajo.engine.function.dataformat;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text to_char(numeric "any", str "pattern")
 */
@Description(
    functionName = "to_char",
    description = "convert number to string.",
    detail = "In a to_char output template string, there are certain patterns that are recognized and replaced with appropriately-formatted data based on the given value.",
    example = "> SELECT to_char(123, '999');\n"
        + "123",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.TEXT})
    }
)
public class ToCharDataFormat extends GeneralFunction {

  public ToCharDataFormat() {
    super(new Column[] {
        new Column("number", TajoDataTypes.Type.NUMERIC),
        new Column("pattern", TajoDataTypes.Type.TEXT)
    });
  }

  boolean hasOthersPattern (int[] commaIndex, String pttn) {
    int cntdot = 0;
    int cntIndex = 0;
    for(int i=0; i<pttn.length(); i++) {
      if(pttn.charAt(i)!='0' && pttn.charAt(i)!='9' && pttn.charAt(i)!=',') {
        if(pttn.charAt(i)=='.') {
          cntdot++;
          if(cntdot>1) {
            return true;
          }
        }
        else {
          return true;
        }
      }
      else if(pttn.charAt(i)==',') {
        commaIndex[cntIndex++]=i;
      }
    }
    return false;
  }



  void getFormatedNumber(StringBuilder result, double originNum, String pttn) {
    long convertNum = (long)originNum;
    String intPart = new String("");
    String realPart = new String("");

    int intPartArrIdx=pttn.indexOf(".");
    if(intPartArrIdx!=-1) {
      intPart=pttn.substring(0,intPartArrIdx);
      realPart=pttn.substring(intPartArrIdx+1,pttn.length());
    }
    else {
      intPart=String.valueOf(pttn);
    }

    long absConverNum = Math.abs(convertNum);
    String tmpPttn = pttn;
    int intPartNum = (String.valueOf(absConverNum)).length();
    int intPartNumLen = intPart.length();
    if( intPartNum > intPartNumLen)
      result.append(tmpPttn.replaceAll("9", "#"));
    else {
      if(intPartNum < intPartNumLen) {
        if(intPart.contains("0")) {
          for(int i=intPartNumLen-intPartNum-1; i>=0; i--) {
            result.append("0");
          }
        }
        else {
          for(int i=intPartNumLen-intPartNum-1; i>=0; i--) {
            if(intPart.charAt(i)=='9') {
              result.append(" ");
            }
          }
        }
      }
      result.append(String.valueOf(absConverNum));
    }

    // Formatted decimal point digits
    if(!realPart.equals("") /*|| tmpNum-(double)dotUpper != 0.*/) {
      int realPartNumLen = realPart.length();

      // Rounding
      String strRealPartNum = String.valueOf(originNum);
      int startIndex = strRealPartNum.indexOf(".");
      strRealPartNum = strRealPartNum.substring(startIndex+1, strRealPartNum.length());
      double underNum = Double.parseDouble(strRealPartNum);
      double realPartNum=underNum * Math.pow(0.1, strRealPartNum.length());

      double roundNum = 0.;
      if(originNum > 0) {
        roundNum = (long)(realPartNum * Math.pow(10, realPartNumLen) + 0.5) / Math.pow(10, realPartNumLen);
      }
      else {
        realPartNum*=-1;
        roundNum = (long)(realPartNum * Math.pow(10, realPartNumLen) - 0.5) / Math.pow(10, realPartNumLen);
      }
      String strRoundNum = String.valueOf(roundNum);

      // Fill decimal point digits
      startIndex = strRoundNum.indexOf(".");
      int endIndex = 0;
      if (strRoundNum.length() >= startIndex+realPartNumLen+1) {
        endIndex = startIndex+realPartNumLen+1;
      }
      else {
        endIndex = strRoundNum.length();
      }
      int dotUnderLen = strRoundNum.substring(startIndex, endIndex).length()-1;
      result.append(strRoundNum.substring(startIndex, endIndex));

      // Fill 0 if Pattern is longer than rounding values
      for(int i=dotUnderLen; i<realPartNumLen; i++) {
        if(realPart.charAt(i)=='0' || realPart.charAt(i)=='9') {
          result.append("0");
        }
      }
    }
  }

  void insertCommaPattern(StringBuilder result, int[] commaIndex, double originNum) {
    int increaseIndex=0;
    if(result.charAt(0)=='-')
      increaseIndex++;
    for (int aCommaIndex : commaIndex) {
      if(aCommaIndex!=-1) {
        if (result.charAt(aCommaIndex - 1 + increaseIndex) == ' ') {
          increaseIndex--;
        } else {
          result.insert(aCommaIndex + increaseIndex, ',');
        }
      }
    }

    int minusIndex = 0;
    if(originNum < 0) {
      for(minusIndex=0;minusIndex<result.length();minusIndex++) {
        if(result.charAt(minusIndex+1)!=' ' || result.charAt(minusIndex)=='0' || result.charAt(minusIndex)=='#') {
          break;
        }
      }
      if(minusIndex==0) {
        result.insert(minusIndex,'-');
      }
      else {
        result.insert(minusIndex+1,'-');
      }
    }
  }

  @Override
  public Datum eval(Tuple params) {
    String num="";
    String pttn="";

    Datum number = params.get(0);
    Datum pattern = params.get(1);
    StringBuilder result = new StringBuilder();

    if(number instanceof NullDatum || pattern instanceof NullDatum) {
      return NullDatum.get();
    }

    num = number.asChars();
    pttn = pattern.asChars();
    int[] commaIndex = new int[pttn.length()];
    for(int i=0;i<commaIndex.length;i++)
      commaIndex[i]=-1;

    if(hasOthersPattern(commaIndex, pttn)) {
      return NullDatum.get();
    }
    //pickCommaPattern
    pttn = pttn.replaceAll(",","");

    double originNum = Double.parseDouble(num);
    getFormatedNumber(result, originNum, pttn);
    insertCommaPattern(result, commaIndex, originNum);

    //paste pattern into Array[keep index];
    return DatumFactory.createText(result.toString());
  }
}