package net.deelam.vertx.jobboard;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferFactoryImpl;
import io.vertx.core.json.Json;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import net.deelam.vertx.KryoMessageCodec;

public class JobDTOTest {

  JobDTO obj;

  @Before
  public void setUp() throws Exception {
    obj = new JobDTO("job1", "type1", new PojoObj());    
  }

  @After
  public void tearDown() throws Exception {}

  @Accessors(chain = true)
  @NoArgsConstructor(access = AccessLevel.PACKAGE)
  @Data
  static class PojoObj {

    List<String> jobs = new ArrayList<>();
    String str;

  }
  
  @Data
  private static class JsonObjectInClass {
    PojoObj pojo = new PojoObj();
    String str;
  }

  @Test
  public void testJsonObjectInClass() {
    JsonObjectInClass list = new JsonObjectInClass();
    //list.pojo.jobs.add("1");
    list.str = "someString";


    String json = Json.encode(list);
    System.out.println(json);
    JsonObjectInClass decObj = Json.decodeValue(json, JsonObjectInClass.class);
    assertEquals(list, decObj);
  }

  @Test
  public void testForEventBus() {
    KryoMessageCodec<JobDTO> codec = new KryoMessageCodec<>(JobDTO.class);
    Buffer buffer = new BufferFactoryImpl().buffer();
    codec.encodeToWire(buffer, obj);

    JobDTO decObj = codec.decodeFromWire(0, buffer);
    assertEquals(obj, decObj);
  }
  

}
