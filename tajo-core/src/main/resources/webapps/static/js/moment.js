/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var Moment = function(millisec, autoIncCallback) {
  this.seconds = millisec;
  this.days = 0;
  this.restHours = 0;
  this.restMinutes = 0;
  this.restSeconds = 0;
  this.interval = null;
  this.opSeconds= function() {
    this.seconds = this.seconds;
  };
  this.opMinutes= function() {
    this.minutes = parseInt(this.seconds / 60);
    this.restSeconds = parseInt(this.seconds % 60);
  };
  this.opHours= function() {
    this.hours = parseInt(this.minutes / 60);
    this.restMinutes = parseInt(this.minutes % 60);
  };
  this.opDays= function() {
    this.days = parseInt(this.hours / 24);
    this.restHours = parseInt(this.hours % 24);
  };
  this.getSeconds= function() {
    return this.restSeconds;
  };
  this.getMinutes= function() {
    return this.restMinutes;
  };
  this.getHours= function() {
    return this.restHours;
  };
  this.getDays= function() {
    return this.days;
  };
  this.start= function() {
    this.opSeconds();
    this.opMinutes();
    this.opHours();
    this.opDays();
  };
  this.start();
  if(autoIncCallback){
    var self = this;
    this.interval = setInterval(function(){
      self.seconds += 1;
      self.start();
      autoIncCallback(self);
    }, 1000);
  }
  this.clearInterval= function(){
    if(this.interval){
      clearInterval(this.interval);
    }
  }

  this.toObject= function() {
    return {
      days: this.getDays(),
      hours: this.getHours(),
      minutes: this.getMinutes(),
      seconds: this.getSeconds()
    };
  };

  this.toString= function() {
    var fillzero = function(val, max){
      if((val+'').length < max){
        val = '0'+ val;
      }
      return val;
    };
    var secs = this.toObject().seconds;
    var mins = this.toObject().minutes;
    var hours= this.toObject().hours;
    var days = this.toObject().days;
    var ret = "";
    if (days > 0) {
      ret = days +' Days';
    }
    if(ret.length>0){
      ret += ' ';
    }
    hours = fillzero(hours, 2);
    ret += hours +':';
    mins = fillzero(mins, 2);
    ret += mins +':';
    secs = fillzero(secs, 2);
    ret += secs;
    return ret;
  };
}