/*
 * Copyright The Apache Software Foundation
 *
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

$(document).ready(
  function(){
    var showChar = 100;  // How many characters are shown by default
    var ellipsestext = "...";
    var moretext = "Show more >";
    var lesstext = "Show less";

    $('.more').each(function() {
      var content = $(this).html();

      if(content.length > showChar) {
        var c = content.substr(0, showChar);
        var h = content.substr(showChar, content.length - showChar);

        var html = c + '<span class="moreellipses">' + ellipsestext
          + '&nbsp;</span><span class="morecontent"><span>' + h
          + '</span>&nbsp;&nbsp;<a href="" class="morelink">'
          + moretext + '</a></span>';

        $(this).html(html);
      }
    });

    $(".morelink").click(function(){
      if($(this).hasClass("less")) {
        $(this).removeClass("less");
        $(this).html(moretext);
      } else {
        $(this).addClass("less");
        $(this).html(lesstext);
      }
      $(this).parent().prev().toggle();
      $(this).prev().toggle();
      return false;
    });

    var prefix = "tab_";
	  $('.tabbable .nav-pills a').click(function (e) {
        e.preventDefault();
        location.hash = $(e.target).attr('href').substr(1).replace(prefix, "");
        $(this).tab('show');
    });
            
    if (location.hash !== '') {
      var tabItem = $('a[href="' + location.hash.replace("#", "#"+prefix) + '"]');
      tabItem.tab('show');
      $(document).scrollTop(0);  
      return false;  
    }
    return true;
  }
);
