package spark1;

import java.util.Properties;

import com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.google.protobuf.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


public class producerApp {
    public static void main (String[] args)
    {
        // set the properties
        //refer ConstantConfigs
        // client id/producer id
        // boot strap server,serializer-to serializer key value
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ConstantConfigs.appID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantConfigs.bootstrapServerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, NumberSerializers.IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer object
        KafkaProducer<Integer, String> producer1 = new KafkaProducer<Integer, String>(props);
        // calling send method
        for (int i=1 ; i<=50 ; i++)
        {
        producer1.send(new ProducerRecord<Integer, String>(ConstantConfigs.topicName, i, "Message numer "+ i));
        }
        // close producer
        producer1.close();


    }
}
