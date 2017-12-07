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
import lombok.Data;
import net.deelam.vertx.KryoMessageCodec;

public class JobListDTOTest {

  JobListDTO obj;
  @Before
  public void setUp() throws Exception {
    List<JobDTO> jobs=new ArrayList<>();
    JobDTO jobDto = new JobDTO("id", "type", null);
    //jobDto.setRequest(new PojoObj());
    jobs.add(jobDto);
    obj=new JobListDTO(jobs);
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testList() {
    List<String> list=new ArrayList<>();
    list.add("1");
    String json = Json.encode(list);
    System.out.println(json);
    List<?> decObj = Json.decodeValue(json, List.class);
    assertEquals(list, decObj);
  }

  @Data
  private static class ListInClass {
    List<String> jobs=new ArrayList<>();
    String str;
  }
  
  @Test
  public void testListInClass() {
    ListInClass list=new ListInClass();
    list.jobs.add("1");
    list.str="someString";
    String json = Json.encode(list);
    System.out.println(json);
    ListInClass decObj = Json.decodeValue(json, ListInClass.class);
    assertEquals(list, decObj);
  }

  @Data
  private static class PojoListInClass {
    List<ListInClass> jobs=new ArrayList<>();
  }
  
  @Test
  public void testPojoListInClass() {
    PojoListInClass list=new PojoListInClass();
    ListInClass sublist=new ListInClass();
    sublist.jobs.add("1");
    sublist.str="someString";
    list.jobs.add(sublist);
    String json = Json.encode(list);
    System.out.println(json);
    PojoListInClass decObj = Json.decodeValue(json, PojoListInClass.class);
    assertEquals(list, decObj);
  }

  @Test
  public void testJsonCodec() {
    String json = Json.encode(obj);
    System.out.println(json);
    JobListDTO decObj = Json.decodeValue(json, JobListDTO.class);
    assertEquals(obj, decObj);
  }

  @Test
  public void testForEventBus() {
    KryoMessageCodec<JobListDTO> codec = new KryoMessageCodec<>(JobListDTO.class);
    Buffer buffer=new BufferFactoryImpl().buffer();
    codec.encodeToWire(buffer, obj);
    
    JobListDTO decObj = codec.decodeFromWire(0, buffer);
    assertEquals(obj, decObj);
  }

}
