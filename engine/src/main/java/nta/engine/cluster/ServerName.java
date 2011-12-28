package nta.engine.cluster;

import java.util.Collection;

public class ServerName implements Comparable<ServerName> {
  /**
   * This character is used as separator between server hostname and port.   * 
   */
  public static final String SERVERNAME_SEPARATOR = ":";

  private final String servername;
  private final String hostname;
  private final int port;


  public ServerName(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
    this.servername = getServerName(hostname, port);
  }

  public ServerName(final String serverName) {
    this(parseHostname(serverName), parsePort(serverName));
  }
  
  public static ServerName create(final String serverName) {
	  return new ServerName(serverName);
  }

  public static String parseHostname(final String serverName) {
    if (serverName == null || serverName.length() <= 0) {
      throw new IllegalArgumentException("Passed hostname is null or empty");
    }
    int index = serverName.indexOf(SERVERNAME_SEPARATOR);
    return serverName.substring(0, index);
  }

  public static int parsePort(final String serverName) {
    String [] split = serverName.split(SERVERNAME_SEPARATOR);
    return Integer.parseInt(split[1]);
  }

  @Override
  public String toString() {
    return getServerName();
  }

  public String getServerName() {
    return servername;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public static String getServerName(String hostName, int port) {
    final StringBuilder name = new StringBuilder(hostName.length() + 4);
    name.append(hostName);
    name.append(SERVERNAME_SEPARATOR);
    name.append(port);
    return name.toString();
  }

  /**
   * @param hostAndPort String in form of &lt;hostname> ':' &lt;port>
   * @param startcode
   * @return Server name made of the concatenation of hostname, port and
   * startcode formatted as <code>&lt;hostname> ',' &lt;port> ',' &lt;startcode></code>
   */
  public static String getServerName(final String hostAndPort) {
    int index = hostAndPort.indexOf(":");
    if (index <= 0) throw new IllegalArgumentException("Expected <hostname> ':' <port>");
    return getServerName(hostAndPort.substring(0, index),
    		Integer.parseInt(hostAndPort.substring(index + 1)));
  }

  @Override
  public int compareTo(ServerName other) {
    int compare = this.getHostname().toLowerCase().
      compareTo(other.getHostname().toLowerCase());
    if (compare != 0) return compare;
    return this.getPort() - other.getPort();        
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof ServerName)) return false;
    return this.compareTo((ServerName)o) == 0;
  }


  /**
   * @return ServerName with matching hostname and port.
   */
  public static ServerName findServerWithSameHostnamePort(final Collection<ServerName> names,
      final ServerName serverName) {
    for (ServerName sn: names) {
      if (serverName.equals(sn)) return sn;
    }
    return null;
  }

  /**
   * @param left
   * @param right
   * @return True if <code>other</code> has same hostname and port.
   */
  public static boolean isSameHostnameAndPort(final ServerName left,
      final ServerName right) {
    if (left == null) return false;
    if (right == null) return false;
    return left.getHostname().equals(right.getHostname()) &&
      left.getPort() == right.getPort();
  }
}