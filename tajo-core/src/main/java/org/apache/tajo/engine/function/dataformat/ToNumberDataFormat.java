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
import java.util.Vector;

/**
 * Function definition
 *
 * text to_char(numeric "any", str "pattern")
 */
@Description(
    functionName = "to_number",
    description = "convert string to number.",
    detail = "In a to_number output template string, there are certain patterns that are recognized and replaced with appropriately-formatted data based on the given value.",
    example = "> SELECT to_number('-12,454.8', '99G999D9S');\n"
        + "-12454.8",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class ToNumberDataFormat extends GeneralFunction {

  public ToNumberDataFormat() {
    super(new Column[] {
        new Column("number", TajoDataTypes.Type.TEXT),
        new Column("pattern", TajoDataTypes.Type.TEXT)
    });
  }

  StringBuilder result = new StringBuilder();
  String num="";
  String pttn="";
  Vector<Integer> commaIndex;

  //int dotUpper=0;

  String dotUpperPttn = "";
  String dotUnderPttn = "";

  boolean hasOthersPattern () {
    int cntdot = 0;
    commaIndex = new Vector<Integer>();
    for(int i=0; i<pttn.length(); i++) {
      if(pttn.charAt(i)!='0' && pttn.charAt(i)!='9') {
        if(pttn.charAt(i)=='.') {
          cntdot++;
          if(cntdot>1)
            return true;
        }
        else
          return true;
      }
    }
    return false;
  }

  void pickCommaPattern() {
    num = num.replaceAll(",","");
  }

  boolean checkFormatedNumber() {
    double tmp=Math.abs(Double.parseDouble(num));
    //dotUpper=(int)tmp;

    int dotIndex=pttn.indexOf(".");
    if(dotIndex!=-1) {
      dotUpperPttn=pttn.substring(0,dotIndex);
      dotUnderPttn=pttn.substring(dotIndex+1,pttn.length());
    }
    else
      dotUpperPttn=String.valueOf(pttn);

    int dotUnderPttnLen = dotUnderPttn.length();

    if(String.valueOf((long)tmp).length() > dotUpperPttn.length())
      return false;
    if(Double.parseDouble(num) < 0)
      result.append("-");
    result.append((long)tmp);

    int tmplength = String.valueOf((double)(tmp-(long)tmp)).length();
    double underNum = tmp-(long)tmp;
    if(dotUnderPttn.length() != 0) {
      result.append(".");
      double roundNum=0.;
      if(Double.parseDouble(num) > 0)
        roundNum = (long)(underNum * Math.pow(10, dotUnderPttnLen) + (double)0.5) / Math.pow(10, dotUnderPttnLen);
      else {
        underNum*=-1;
        roundNum = (long)(underNum * Math.pow(10, dotUnderPttnLen) - (double)0.5) / Math.pow(10, dotUnderPttnLen);
      }
      roundNum=roundNum*Math.pow(10, dotUnderPttnLen);
      if(roundNum > 0)
        result.append((long)roundNum);
      else {
        roundNum*=-1;
        result.append((long)roundNum);
      }
    }

    return true;
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
    pickCommaPattern();

    if(!checkFormatedNumber())
      return NullDatum.get();

    double tmpNum=Double.parseDouble(num);
    if((double)(Double.parseDouble(result.toString()))-(long)(Double.parseDouble(result.toString()))!=0.)
      return DatumFactory.createFloat8(result.toString());
    else
      return DatumFactory.createInt8(result.toString());
  }
}