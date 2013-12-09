package com.xiaomi.infra.hbase;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.jetty.HttpStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple agent to send counter to OWL
 * @author liushaohui
 */
public class Agent {
  public class Counter {
    private String key;
    private String unit;
    private double value;

    public Counter(String key, String unit, double value) {
      super();
      this.key = key;
      this.unit = unit;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getUnit() {
      return unit;
    }

    public void setUnit(String unit) {
      this.unit = unit;
    }

    public double getValue() {
      return this.value;
    }

    public void setValue(double value) {
      this.value = value;
    }
  }

  private static final Log LOG = LogFactory.getLog(Agent.class);

  private String uri;
  private String group;
  private HttpClient client = new HttpClient();
  private List<Counter> counters = new ArrayList<Counter>();

  public Agent(final String group, final String uri) {
    this.group = group;
    this.uri = uri;
  }

  public void sendCounters() {
    PostMethod post = new PostMethod(this.uri);
    JSONArray arrays = new JSONArray();
    for (Counter counter : counters) {
      JSONObject json = new JSONObject();
      try {
        json.put("group", this.group);
        json.put("name", counter.getKey());
        json.put("unit", counter.getUnit());
        json.put("value", counter.getValue());
      } catch (JSONException e) {
        LOG.error("Put json error", e);
        counters.clear();
        return;
      }
      arrays.put(json);
    }
    post.setRequestEntity(new StringRequestEntity(arrays.toString()));
    try {
      int statusCode = this.client.executeMethod(post);
      if (statusCode != HttpStatus.ORDINAL_200_OK) {
        LOG.error("Send counters to " + uri + " error. Http statusCode:"
            + statusCode);
      }
    } catch (HttpException e) {
      LOG.error("Send counters error", e);
    } catch (IOException e) {
      LOG.error("Send counters error", e);
    }
    counters.clear();
  }

  public void write(String metric, String unit, double value)
      throws IOException {
    counters.add(new Counter(metric, unit, value));
  }

  public void close() throws IOException {
    this.sendCounters();
  }
}
