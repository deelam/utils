package net.deelam.vertx.rpc;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;

import io.vertx.core.buffer.Buffer;
import net.deelam.vertx.rpc.OperationParam.ValuetypeEnum;
import net.deelam.vertx.rpc.VertxRpcUtil.KryoSerDe;

public class KryoSerDeTest {

  KryoSerDe kryo = new KryoSerDe("test");

  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void testWriteException() {
    CompletionException except = new CompletionException(
        (new IllegalArgumentException("blah ")));
    Buffer buf = kryo.writeObject(except);
    Registration c = kryo.readClass(new Input(buf.getBytes()));
    Throwable ex = kryo.readObject(buf);
    System.out.println(buf.getBytes().length + " " + c + ", " + ex.getClass() + ", " + ex);
  }

  @Test
  public void testMethodId() {
    HashMap<String, Method> methods = new HashMap<>();
    for (Method method : KryoSerDe.class.getDeclaredMethods()) {
      method.setAccessible(true);
      methods.put(VertxRpcUtil.genMethodId(method), method);
    }
    System.out.println(methods.toString().replaceAll(", ", "\n"));
  }


  @Test
  public void testWriteOperationList() {
    Operation op1;
    {
      List<OperationParam> params = new ArrayList<>();
      Map<String, String> info = new HashMap<>();
      info.put(OperationConsts.OPERATION_TYPE, OperationConsts.TYPE_INGESTER);
      op1 = (new Operation().id("ADD_SOURCE_DATASET" + System.currentTimeMillis()).description("add source dataset")
          .info(info)
          .params(params));
      params.add(new OperationParam().key("inputUri").required(true).description("location of source dataset")
          .valuetype(ValuetypeEnum.STRING).defaultValue(null).isMultivalued(false));
      params.add(new OperationParam().key(OperationConsts.DATA_FORMAT).required(true)
          .description("type and format of data")
          .valuetype(ValuetypeEnum.ENUM).defaultValue(null).isMultivalued(false)
          .addPossibleValuesItem("TELEPHONE.CSV").addPossibleValuesItem("PEOPLE.CSV"));
    }
    Operation op2;
    {
      List<OperationParam> params = new ArrayList<>();
      Map<String, String> info = new HashMap<>();
      info.put(OperationConsts.OPERATION_TYPE, OperationConsts.TYPE_INGESTER);
      op2 = (new Operation().id("ADD_SOURCE_DATASET" + System.currentTimeMillis()).description("add source dataset")
          .info(info)
          .params(params));
      params.add(new OperationParam().key("inputUri").required(true).description("location of source dataset")
          .valuetype(ValuetypeEnum.STRING).defaultValue(null).isMultivalued(false));
      params.add(new OperationParam().key(OperationConsts.DATA_FORMAT).required(true)
          .description("type and format of data")
          .valuetype(ValuetypeEnum.ENUM).defaultValue(null).isMultivalued(false)
          .addPossibleValuesItem("TELEPHONE.CSV").addPossibleValuesItem("PEOPLE.CSV"));

    }
    List<Operation> opList = Lists.newArrayList(op1, op2);
    Buffer buf = kryo.writeObject(opList);
    Registration c = kryo.readClass(new Input(buf.getBytes()));
    List ex = kryo.readObject(buf);
    System.out.println(buf.getBytes().length + " " + c + ", " + ex.getClass() + ", " + ex);
  }
}
