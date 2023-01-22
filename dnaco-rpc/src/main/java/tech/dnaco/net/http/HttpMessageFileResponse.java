package tech.dnaco.net.http;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import tech.dnaco.data.DataFormat;
import tech.dnaco.dispatcher.message.Message;
import tech.dnaco.dispatcher.message.MessageMetadata;

public class HttpMessageFileResponse implements Message {
  private final HttpMessageMetadata metadata;
  private final DefaultHttpResponse response;
  private final DefaultFileRegion region;
  private final long timestamp;

  public HttpMessageFileResponse(final DefaultHttpResponse response, final DefaultFileRegion region) {
    this.response = response;
    this.metadata = new HttpMessageMetadata(response.headers());
    this.region = region;
    this.timestamp = System.nanoTime();
  }

  @Override
  public Message retain() {
    //response.retain();
    region.retain();
    return this;
  }

  @Override
  public Message release() {
    region.release();
    //response.release();
    return this;
  }

  @Override
  public long timestampNs() {
    return timestamp;
  }

  @Override
  public int contentLength() {
    return -1;
  }

  @Override
  public long writeContentToStream(final OutputStream stream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long writeContentToStream(final DataOutput stream) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T convertContent(final DataFormat format, final Class<T> classOfT) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int estimateSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MessageMetadata metadata() {
    return metadata;
  }

  protected void write(final ChannelHandlerContext ctx) {
    System.out.println("---> WRITE");
    ctx.write(response, ctx.channel().voidPromise());
    ctx.write(region, ctx.channel().voidPromise());
    ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
  }

  @Sharable
  public static final class HttpMessageFileResponseEncoder extends MessageToMessageEncoder<HttpMessageFileResponse> {
    public static final HttpMessageFileResponseEncoder INSTANCE = new HttpMessageFileResponseEncoder();

    private HttpMessageFileResponseEncoder() {
      // no-op
    }

    @Override
    protected void encode(final ChannelHandlerContext ctx, final HttpMessageFileResponse msg, final List<Object> out) throws Exception {
      out.add(msg.response);
      out.add(msg.region);
      out.add(LastHttpContent.EMPTY_LAST_CONTENT);
    }
  }
}
