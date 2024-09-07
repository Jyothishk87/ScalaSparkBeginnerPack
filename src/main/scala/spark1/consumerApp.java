package spark1;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Properties;

import com.fasterxml.jackson.databind.deser.std.NumberDeserializers;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class consumerApp {
    public static void main (String[] args)
    {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, ConstantConfigs.appID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ConstantConfigs.bootstrapServerList);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, NumberDeserializers.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,"GROUP-1");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // consumer object
        KafkaConsumer<Integer, String> consumer1 = new KafkaConsumer<Integer,String>(consumerProps);
        consumer1.subscribe(Arrays.asList("all_orders"));
        //
    }
}
