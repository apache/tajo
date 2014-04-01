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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.FunctionEval;
import org.apache.tajo.engine.function.GeneralFunction;
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
        + "125",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.TEXT})
    }
)
public class ToCharDataFormat extends GeneralFunction {
  @Expose private boolean hasMoreCharacters;

  public ToCharDataFormat() {
    super(new Column[] {
        new Column("number", TajoDataTypes.Type.NUMERIC),
        new Column("pattern", TajoDataTypes.Type.TEXT)
    });
  }

  StringBuilder result = new StringBuilder();
  String num="";
  String pttn="";

  int dotUpper=0;
  int dotUnder=0;

  String dotUpperPttn = "";
  String dotUnderPttn = "";

  void getDigit() {
    for(int i = 0; i < num.length(); i++){
      if( Character.isDigit( num.charAt(i) ) ) {
        result.append( num.charAt(i) );
      }
      else if (num.charAt(i)=='.') {
        result.append(num.charAt(i));
      }
    }
  }

  boolean hasOthersPattern () {
    for(int i=0; i<pttn.length(); i++) {
      if(pttn.charAt(i)!='0' && pttn.charAt(i)!='9' && pttn.charAt(i)!='.')
        return true;
    }
    return false;
  }

  void getFormatedNumber() {
    double tmp=Double.parseDouble(num);
    dotUpper=(int)tmp;

    int dotIndex=pttn.indexOf(".");
    if(dotIndex!=-1) {
      dotUpperPttn=pttn.substring(0,dotIndex);
      dotUnderPttn=pttn.substring(dotIndex+1,pttn.length());
    }
    else
      dotUpperPttn=String.valueOf(pttn);

    int tmpUpper=Math.abs(dotUpper);
    String tmpPttn = new String(pttn);
    int tmpUpperLen = (String.valueOf(tmpUpper)).length();
    int dotUpperPttrnLen = dotUpperPttn.length();
    if( tmpUpperLen > dotUpperPttrnLen) {
      if(dotUpper < 0)
        result = result.append("-");
      result  = result.append(tmpPttn.replace("9", "#"));
    }
    else {
      if(tmpUpperLen < dotUpperPttrnLen) {
        if(dotUpper < 0)
          result = result.append("-");

        for(int i=dotUpperPttrnLen-tmpUpperLen-1; i>=0; i--) {
          if(dotUpperPttn.charAt(i)=='0')
            result = result.append("0");
          else if(dotUpperPttn.charAt(i)=='9')
            result = result.append(" ");
        }
      }
      result = result.append(String.valueOf(tmpUpper));
    }

    double tmpNum=Double.parseDouble(num);

    // Formatted decimal point digits
    if(tmpNum-(double)dotUpper != 0.) {
      int dotUnderPttnLen = dotUnderPttn.length();

      // Rounding
      double roundNum = (int)(tmpNum * Math.pow(10, dotUnderPttnLen) + 0.5f) / Math.pow(10, dotUnderPttnLen);
      String strRoundNum = String.valueOf(roundNum);

      // Fill decimal point digits
      int startIndex = strRoundNum.indexOf(".");
      int endIndex = 0;
      if (strRoundNum.length() >= startIndex+dotUnderPttnLen+1)
        endIndex = startIndex+dotUnderPttnLen+1;
      else
        endIndex = strRoundNum.length();
      int dotUnderLen = strRoundNum.substring(startIndex, endIndex).length()-1;
      result = result.append(strRoundNum.substring(startIndex, endIndex));

      // Fill 0 if Pattern is longer than rounding values
      for(int i=dotUnderLen; i<dotUnderPttnLen; i++) {
        if(dotUnderPttn.charAt(i)=='0' || dotUnderPttn.charAt(i)=='9')
          result = result.append("0");
      }
    }
  }

  @Override
  public Datum eval(Tuple params) {
    Datum number = params.get(0);
    Datum pattern = params.get(1);

    if(number instanceof NullDatum || pattern instanceof NullDatum) return NullDatum.get();

    num = number.asChars();
    pttn = pattern.asChars();

    if(hasOthersPattern())
      return NullDatum.get();

    getFormatedNumber();

    //paste pattern into Array[keep index];
    return DatumFactory.createText(result.toString());
  }
}