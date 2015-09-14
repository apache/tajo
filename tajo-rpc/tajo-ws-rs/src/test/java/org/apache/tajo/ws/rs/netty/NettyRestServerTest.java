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

package org.apache.tajo.ws.rs.netty;

import org.apache.tajo.ws.rs.netty.gson.GsonFeature;
import org.apache.tajo.ws.rs.netty.testapp1.TestApplication1;
import org.apache.tajo.ws.rs.netty.testapp1.TestResource1;
import org.apache.tajo.ws.rs.netty.testapp2.Directory;
import org.apache.tajo.ws.rs.netty.testapp2.FileManagementApplication;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Collection;

import static org.junit.Assert.*;

public class NettyRestServerTest {

  @Test
  public void testNettyRestServerCreation() throws Exception {
    ResourceConfig resourceConfig = ResourceConfig.forApplicationClass(TestApplication1.class);
    ServerSocket serverSocket = new ServerSocket(0);
    int availPort = serverSocket.getLocalPort();
    serverSocket.close();
    URI baseUri = new URI("http://localhost:"+availPort+"/rest");

    NettyRestServer restServer = NettyRestServerFactory.createNettyRestServer(baseUri, resourceConfig, 3);

    assertNotNull(restServer);
    assertNotNull(restServer.getHandler());
    assertNotNull(restServer.getChannel());
    assertNotNull(restServer.getListenAddress());

    InetSocketAddress listeningAddress = restServer.getListenAddress();

    assertEquals(availPort, listeningAddress.getPort());
  }

  @Test
  public void testTextPlainApplication() throws Exception {
    ResourceConfig resourceConfig = ResourceConfig.forApplicationClass(TestApplication1.class);
    ServerSocket serverSocket = new ServerSocket(0);
    int availPort = serverSocket.getLocalPort();
    serverSocket.close();
    URI baseUri = new URI("http://localhost:"+availPort+"/rest");

    NettyRestServer restServer = NettyRestServerFactory.createNettyRestServer(baseUri, resourceConfig, 3);

    try {
      WebTarget webTarget = ClientBuilder.newClient().target(baseUri + "/testapp1");

      assertEquals(TestResource1.outputMessage, webTarget.request(MediaType.TEXT_PLAIN).get(String.class));
    } finally {
      restServer.shutdown();
    }
  }

  protected Directory createDirectory1() {
    Directory newDirectory = new Directory();

    newDirectory.setName("newdir1");
    newDirectory.setOwner("owner1");
    newDirectory.setGroup("group1");

    return newDirectory;
  }

  @Test
  public void testFileMgmtApplication() throws Exception {
    ResourceConfig resourceConfig = ResourceConfig.forApplicationClass(FileManagementApplication.class)
        .register(GsonFeature.class);
    ServerSocket serverSocket = new ServerSocket(0);
    int availPort = serverSocket.getLocalPort();
    serverSocket.close();
    URI baseUri = new URI("http://localhost:"+availPort+"/rest");
    URI directoriesUri = new URI(baseUri + "/directories");
    Client restClient = ClientBuilder.newBuilder()
        .register(GsonFeature.class).build();

    NettyRestServer restServer = NettyRestServerFactory.createNettyRestServer(baseUri, resourceConfig, 3);

    try {
      Directory directory1 = createDirectory1();
      Directory savedDirectory = restClient.target(directoriesUri)
          .request().post(Entity.entity(directory1, MediaType.APPLICATION_JSON_TYPE), Directory.class);

      assertNotNull(savedDirectory);
      assertNotNull(savedDirectory.getName());

      Directory fetchedDirectory = restClient.target(directoriesUri).path("{name}")
          .resolveTemplate("name", directory1.getName()).request().get(Directory.class);

      assertEquals(directory1.getName(), fetchedDirectory.getName());
      assertEquals(directory1.getOwner(), fetchedDirectory.getOwner());
      assertEquals(directory1.getGroup(), fetchedDirectory.getGroup());

      GenericType<Collection<Directory>> directoryType = new GenericType<>(Collection.class);
      Collection<Directory> directories = restClient.target(directoriesUri).request().get(directoryType);

      assertEquals(1, directories.size());

      restClient.target(directoriesUri).path("{name}").resolveTemplate("name", directory1.getName())
          .request().delete();

      directories = restClient.target(directoriesUri).request().get(directoryType);

      assertTrue(directories.isEmpty());
    } finally {
      restServer.shutdown();
    }
  }
}
