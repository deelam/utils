package net.deelam.vertx;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class KryoMessageCodec<C> implements MessageCodec<C, C> {

  public synchronized static <C> void register(EventBus eb, Class<C> clazz) {
    try {
      eb.registerDefaultCodec(clazz, new KryoMessageCodec<C>(clazz));// throws error if already registered
    } catch (IllegalStateException e) {
      // no worries as long as it is registered, keep going
    }
  }

  private final Kryo kryo = new Kryo();
  private final Class<C> clazz;

  @Override
  public synchronized void encodeToWire(Buffer buffer, C msgBody) {
    log.trace("Encoding {}", msgBody);
    Output out = new Output(new ByteArrayOutputStream());
    kryo.writeObject(out, msgBody);

    buffer.appendInt(out.getBuffer().length);
    buffer.appendBytes(out.getBuffer());
  }

  @Override
  public synchronized C decodeFromWire(int pos, Buffer buffer) {
    // My custom message starting from this *pos* of buffer
    int _pos = pos;
    int length = buffer.getInt(_pos);

    // Jump 4 because getInt() == 4 bytes
    byte[] serialized = buffer.getBytes(_pos += 4, _pos += length);
    C o = kryo.readObject(new Input(serialized), clazz);
    log.trace("Decoded {}", o);
    return o;
  }

  @Override
  public synchronized C transform(C body) {
    return kryo.copy(body);
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName() + ":" + clazz.getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }

}
