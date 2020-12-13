package tech.dnaco.storage.net;

import com.gullivernet.server.netty.dnaco.DnacoFrame;
import com.gullivernet.server.netty.dnaco.DnacoServiceSession;
import com.gullivernet.server.netty.dnaco.packets.DnacoBlobPacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoPingPacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoPublishPacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoResultPacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoServicePacketHandler.DnacoServicePacketListener;
import com.gullivernet.server.netty.dnaco.packets.DnacoSubscribePacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoUnsubscribePacket;
import com.gullivernet.server.netty.dnaco.packets.DnacoUriPacket;

public class StorageServiceHandler implements DnacoServicePacketListener {

  @Override
  public void connect(final DnacoServiceSession session) {
    // TODO Auto-generated method stub
  }

  @Override
  public void disconnect(final DnacoServiceSession session) {
    // TODO Auto-generated method stub
  }

  @Override
  public void pingReceived(final DnacoServiceSession session, final DnacoPingPacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void uriReceived(final DnacoServiceSession session, final DnacoUriPacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void blobReceived(final DnacoServiceSession session, final DnacoBlobPacket packet) {
    // no-op
  }

  @Override
  public void resultReceived(final DnacoServiceSession session, final DnacoResultPacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void publishReceived(final DnacoServiceSession session, final DnacoPublishPacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void subscribeReceived(final DnacoServiceSession session, final DnacoSubscribePacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void unsubscribeReceived(final DnacoServiceSession session, final DnacoUnsubscribePacket packet) {
    // TODO Auto-generated method stub
  }

  @Override
  public void unknownFrameReceived(final DnacoServiceSession session, final DnacoFrame frame) {
    session.disconnect();
  }
}
