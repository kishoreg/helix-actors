package org.apache.helix.ipc.benchmark;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.helix.HelixManager;
import org.apache.helix.ipc.HelixIPCCallback;
import org.apache.helix.ipc.HelixIPCMessageCodec;
import org.apache.helix.ipc.netty.NettyHelixIPCService;
import org.apache.helix.resolver.HelixAddress;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.helix.resolver.HelixMessageScope.Builder;
import org.apache.helix.resolver.HelixResolver;

/**
 * Sends and receives data to/from another server
 */
public class TestServer {
  // TestServer localPort remoteHost remotePort
  public static void main(String[] args) throws Exception {
    int localPort = Integer.parseInt(args[0]);
    String remoteHost = args[1];
    int remotePort = Integer.parseInt(args[2]);

    HelixIPCMessageCodec codec = new MyCodec();
    Map<String, InetSocketAddress> map = new HashMap<String, InetSocketAddress>();
    map.put(remoteHost + "_" + remotePort, new InetSocketAddress(InetAddress.getByName(remoteHost),
        remotePort));
    final NettyHelixIPCService helixIpc =
        new NettyHelixIPCService("localhost_" + localPort, localPort);
    final MyResolver resolver = new MyResolver(map);
    HelixIPCCallback callback = new MyCallback(helixIpc, resolver, localPort);
    final int messageType = 1025;
    helixIpc.registerMessageCodec(messageType, codec);
    helixIpc.registerCallback(messageType, callback);

    helixIpc.start();
    Thread[] threads = new Thread[1];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {

        @Override
        public void run() {
          Builder builder = new HelixMessageScope.Builder();
          HelixMessageScope scope =
              builder.cluster("myCluster").resource("myResource").partition("myPartition")
                  .state("Master").build();
          for (int i = 0; i < 10; i++) {
            UUID messageId = UUID.randomUUID();
            MyMessage message = new MyMessage("message" + messageId);
            Set<HelixAddress> destinations = resolver.getDestinations(scope);
            helixIpc.send(destinations, messageType, messageId, message);
          }
        }
      });

    }
    Thread.sleep(6000);
    for (int i = 0; i < threads.length; i++) {
      threads[i].start();
    }
  }
}

class MyCallback implements HelixIPCCallback {

  private int _id;
  private NettyHelixIPCService _helixIpc;
  private MyResolver _resolver;

  public MyCallback(NettyHelixIPCService helixIpc, MyResolver resolver, int id) {
    _helixIpc = helixIpc;
    _resolver = resolver;
    _id = id;
  }

  @Override
  public void onMessage(HelixMessageScope scope, UUID messageId, Object message) {
    System.out.println("MyCallback.onMessage() " + _id);
    HelixAddress source = _resolver.getSource(scope);
    _helixIpc.ack(source, messageId);
  }

}

class MyResolver implements HelixResolver {

  private Map<String, InetSocketAddress> _map;

  public MyResolver(Map<String, InetSocketAddress> map) {
    _map = map;
  }

  @Override
  public void connect() {
    // no-op
  }

  @Override
  public void disconnect() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isConnected() {
    // TODO Auto-generated method stub
    return true;
  }

  @Override
  public Set<HelixAddress> getDestinations(HelixMessageScope scope) {
    Set<HelixAddress> addressSet = new HashSet<HelixAddress>();
    for (String instanceName : _map.keySet()) {
      HelixAddress address = new HelixAddress(scope, instanceName, _map.get(instanceName));
      addressSet.add(address);
    }

    return addressSet;
  }

  @Override
  public HelixAddress getSource(HelixMessageScope scope) {
    String instanceName = scope.getSourceInstance();
    return new HelixAddress(scope, instanceName, _map.get(instanceName));
  }
}

class MyCodec implements HelixIPCMessageCodec {

  @Override
  public MyMessage decode(ByteBuf buf) {
    byte[] bytes = new byte[buf.capacity()];
    buf.readBytes(bytes);
    return new MyMessage(bytes);
  }

  @Override
  public ByteBuf encode(Object message) {
    return Unpooled.wrappedBuffer(((MyMessage) message).rawPaylod);
  }

}

class MyMessage {
  public MyMessage(byte[] rawBytes) {
    rawPaylod = rawBytes;
  }

  public MyMessage(String msgString) {
    rawPaylod = msgString.getBytes();
  }

  byte[] rawPaylod;
}
