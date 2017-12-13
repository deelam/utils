package net.deelam.activemq.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.deelam.activemq.MQClient;

@Slf4j
public class AmqComponentSubscriber {
  private Session session;
  private MessageConsumer consumer;
  private MessageProducer producer;

  public AmqComponentSubscriber(Connection connection, String componentName) throws JMSException {
    this(connection, componentName, (Map<String,Object>) null);
  }
  
  public AmqComponentSubscriber(Connection connection, String componentName, String... attribs) throws JMSException {
    this(connection, componentName, toMap(attribs));
  }
  
  private static Map<String,Object> toMap(String[] attribs) {
    Map<String, Object> attribMap=Maps.newHashMap();
    for(int i=0; i<attribs.length; i=i+2) {
      attribMap.put(attribs[i], attribs[i+1]);
    }
    return attribMap;
  }

  public AmqComponentSubscriber(Connection connection, String componentName, Map<String,Object> attributes) throws JMSException {
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    producer = MQClient.createGenericMsgResponder(session, DeliveryMode.NON_PERSISTENT);
    listen(AmqComponentRegistry.REGISTRY_SUBSCRIBER_TOPIC, componentName, attributes);
  }
  
  private void listen(String topicName, String componentName, Map<String,Object> attributes) {
    try {
      //KryoSerDe serde = new KryoSerDe(session);
      MQClient.createTopicConsumer(session, topicName, message -> {
        int cmdCode = message.getIntProperty(AmqComponentRegistry.COMMAND);
        Message response;
        switch(cmdCode) {
          case AmqComponentRegistry.GET_ATTRIBS:
            log.info("AMQ: sending attributes for: {}", componentName);
            MapMessage mresponse = session.createMapMessage();
            mresponse.setString("componentName", componentName);
            try {
              mresponse.setString("hostname", InetAddress.getLocalHost().getHostName());
            } catch (UnknownHostException e1) {
              //mresponse.setString("hostname", "unknown");
            }
            if(attributes!=null)
              for(Entry<String, Object> e:attributes.entrySet()) {
                mresponse.setObject(e.getKey(), e.getValue());
              }
            response=mresponse;
            break;
          case AmqComponentRegistry.GET_NAMES:
          default:
            log.info("AMQ: sending my component name: {}", componentName);
            response = session.createTextMessage(componentName);
            break;
//          String command = message.getStringProperty(OperationsRegistry_I.COMMAND_PARAM);
//          switch (OPERATIONS_REG_API.valueOf(command)) {
//            case GET_OPERATIONS: {
//              ArrayList<Operation> list = new ArrayList<>();
//              for (Worker_I worker : workers)
//                list.add(worker.operation());
//              BytesMessage response = serde.writeObject(list);
//              response.setJMSCorrelationID(message.getJMSCorrelationID());
//              log.info("Sending response: {}", response);
//              producer.send(message.getJMSReplyTo(), response);
//              break;
//            }
//            default:
//              log.warn("Unknown request: {}", message);
//          }
        }
        response.setJMSCorrelationID(message.getJMSCorrelationID());
        producer.send(message.getJMSReplyTo(), response);
      });

    } catch (Exception e) {
      log.error("When setting up OperationsSubscriber:", e);
    }
  }
}
