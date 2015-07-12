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

package org.apache.tajo.ws.rs.resources;

import org.apache.commons.codec.binary.Base64;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.requests.NewSessionRequest;
import org.apache.tajo.ws.rs.requests.SubmitQueryRequest;
import org.apache.tajo.ws.rs.responses.GetQueryResultDataResponse;
import org.apache.tajo.ws.rs.responses.NewSessionResponse;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.filter.LoggingFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.List;

import static org.junit.Assert.*;

public class TestQueryResultResource extends QueryTestCaseBase {

  private URI restServiceURI;
  private URI sessionsURI;
  private URI queriesURI;
  private Client restClient;

  private static final String tajoSessionIdHeaderName = "X-Tajo-Session";
  private static final String tajoDigestHeaderName = "X-Tajo-Digest";
	private static final String tajoOffsetHeaderName = "X-Tajo-Offset";
	private static final String tajoCountHeaderName = "X-Tajo-Count";
	private static final String tajoEOSHeaderName = "X-Tajo-EOS";

  public TestQueryResultResource() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Before
  public void setUp() throws Exception {
    int restPort = testBase.getTestingCluster().getConfiguration().getIntVar(ConfVars.REST_SERVICE_PORT);
    restServiceURI = new URI("http", null, "127.0.0.1", restPort, "/rest", null, null);
    sessionsURI = new URI(restServiceURI + "/sessions");
    queriesURI = new URI(restServiceURI + "/queries");
    restClient = ClientBuilder.newBuilder()
        .register(new GsonFeature(RestTestUtils.registerTypeAdapterMap()))
        .register(LoggingFilter.class)
        .property(ClientProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .property(ClientProperties.METAINF_SERVICES_LOOKUP_DISABLE, true)
        .build();
  }

  @After
  public void tearDown() throws Exception {
    restClient.close();
  }

  private String generateNewSessionAndGetId() throws Exception {
    NewSessionRequest request = new NewSessionRequest();
    request.setUserName("tajo-user");
    request.setDatabaseName(TajoConstants.DEFAULT_DATABASE_NAME);

    NewSessionResponse response = restClient.target(sessionsURI)
        .request().post(Entity.entity(request, MediaType.APPLICATION_JSON), NewSessionResponse.class);

    assertNotNull(response);
    assertTrue(ResultCode.OK.equals(response.getResultCode()));
    assertTrue(response.getId() != null && !response.getId().isEmpty());

    return response.getId();
  }

  private URI sendNewQueryResquest(String sessionId, String query) throws Exception {

    SubmitQueryRequest request = new SubmitQueryRequest();
    request.setQuery(query);

    Response response = restClient.target(queriesURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .post(Entity.entity(request, MediaType.APPLICATION_JSON));

    assertNotNull(response);
    assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    String locationHeader = response.getHeaderString("Location");
    assertTrue(locationHeader != null && !locationHeader.isEmpty());

    URI queryIdURI = new URI(locationHeader);

    assertNotNull(queryIdURI);

    return queryIdURI;
  }

  @Test
  public void testGetQueryResult() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    URI queryIdURI = sendNewQueryResquest(sessionId, "select * from lineitem");
    URI queryResultURI = new URI(queryIdURI + "/result");

    GetQueryResultDataResponse response = restClient.target(queryResultURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get(new GenericType<GetQueryResultDataResponse>(GetQueryResultDataResponse.class));

    assertNotNull(response);
    assertNotNull(response.getResultCode());
    assertEquals(ResultCode.OK, response.getResultCode());
    assertNotNull(response.getSchema());
    assertEquals(16, response.getSchema().getRootColumns().size());
    assertNotNull(response.getResultset());
    assertTrue(response.getResultset().getId() != 0);
    assertNotNull(response.getResultset().getLink());
  }

  @Test
  public void testGetQueryResultNotFound() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    URI queryIdURI = new URI(queriesURI + "/q_11111_0001");
    URI queryResultURI = new URI(queryIdURI + "/result");

    Response response = restClient.target(queryResultURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get();

    assertNotNull(response);
    assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetQueryResultSet() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    URI queryIdURI = sendNewQueryResquest(sessionId, "select * from lineitem");
    URI queryResultURI = new URI(queryIdURI + "/result");

    GetQueryResultDataResponse response = restClient.target(queryResultURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get(new GenericType<GetQueryResultDataResponse>(GetQueryResultDataResponse.class));

    assertNotNull(response);
    assertNotNull(response.getResultCode());
    assertEquals(ResultCode.OK, response.getResultCode());
    assertNotNull(response.getSchema());
    assertEquals(16, response.getSchema().getRootColumns().size());
    assertNotNull(response.getResultset());
    assertTrue(response.getResultset().getId() != 0);
    assertNotNull(response.getResultset().getLink());

    URI queryResultSetURI = response.getResultset().getLink();

    Response queryResultSetResponse = restClient.target(queryResultSetURI)
        .queryParam("count", 100)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get();

    assertNotNull(queryResultSetResponse);
    String tajoDigest = queryResultSetResponse.getHeaderString(tajoDigestHeaderName);
    assertTrue(tajoDigest != null && !tajoDigest.isEmpty());

    DataInputStream queryResultSetInputStream =
        new DataInputStream(new BufferedInputStream(queryResultSetResponse.readEntity(InputStream.class)));

    assertNotNull(queryResultSetInputStream);

    boolean isFinished = false;
    List<Tuple> tupleList = TUtil.newList();
    RowStoreUtil.RowStoreDecoder decoder = RowStoreUtil.createDecoder(response.getSchema());
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    while (!isFinished) {
      try {
        int length = queryResultSetInputStream.readInt();
        byte[] dataByteArray = new byte[length];
        int readBytes = queryResultSetInputStream.read(dataByteArray);

        assertEquals(length, readBytes);

        tupleList.add(decoder.toTuple(dataByteArray));
        messageDigest.update(dataByteArray);
      } catch (EOFException eof) {
        isFinished = true;
      }
    }

    assertEquals(5, tupleList.size());
    assertEquals(tajoDigest, Base64.encodeBase64String(messageDigest.digest()));

    for (Tuple aTuple: tupleList) {
      assertTrue(aTuple.getInt4(response.getSchema().getColumnId("l_orderkey")) > 0);
    }
  }

  @Test
  public void testGetQueryResultSetWithDefaultCount() throws Exception {
    String sessionId = generateNewSessionAndGetId();
    URI queryIdURI = sendNewQueryResquest(sessionId, "select * from lineitem");
    URI queryResultURI = new URI(queryIdURI + "/result");

    GetQueryResultDataResponse response = restClient.target(queryResultURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get(new GenericType<GetQueryResultDataResponse>(GetQueryResultDataResponse.class));

    assertNotNull(response);
    assertNotNull(response.getResultCode());
    assertEquals(ResultCode.OK, response.getResultCode());
    assertNotNull(response.getSchema());
    assertEquals(16, response.getSchema().getRootColumns().size());
    assertNotNull(response.getResultset());
    assertTrue(response.getResultset().getId() != 0);
    assertNotNull(response.getResultset().getLink());

    URI queryResultSetURI = response.getResultset().getLink();

    Response queryResultSetResponse = restClient.target(queryResultSetURI)
        .request().header(tajoSessionIdHeaderName, sessionId)
        .get();

    assertNotNull(queryResultSetResponse);
    String tajoDigest = queryResultSetResponse.getHeaderString(tajoDigestHeaderName);
    int offset = Integer.valueOf(queryResultSetResponse.getHeaderString(tajoOffsetHeaderName));
    int count = Integer.valueOf(queryResultSetResponse.getHeaderString(tajoCountHeaderName));
    boolean eos = Boolean.valueOf(queryResultSetResponse.getHeaderString(tajoEOSHeaderName));

    assertTrue(tajoDigest != null && !tajoDigest.isEmpty());
    assertTrue(eos);
    assertEquals(0, offset);
    assertEquals(5, count);

    DataInputStream queryResultSetInputStream =
        new DataInputStream(new BufferedInputStream(queryResultSetResponse.readEntity(InputStream.class)));

    assertNotNull(queryResultSetInputStream);

    boolean isFinished = false;
    List<Tuple> tupleList = TUtil.newList();
    RowStoreUtil.RowStoreDecoder decoder = RowStoreUtil.createDecoder(response.getSchema());
    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    while (!isFinished) {
      try {
        int length = queryResultSetInputStream.readInt();
        byte[] dataByteArray = new byte[length];
        int readBytes = queryResultSetInputStream.read(dataByteArray);

        assertEquals(length, readBytes);

        tupleList.add(decoder.toTuple(dataByteArray));
        messageDigest.update(dataByteArray);
      } catch (EOFException eof) {
        isFinished = true;
      }
    }

    assertEquals(5, tupleList.size());
    assertEquals(tajoDigest, Base64.encodeBase64String(messageDigest.digest()));

    for (Tuple aTuple: tupleList) {
      assertTrue(aTuple.getInt4(response.getSchema().getColumnId("l_orderkey")) > 0);
    }
  }
}
