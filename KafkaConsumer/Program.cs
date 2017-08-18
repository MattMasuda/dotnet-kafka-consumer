using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapList = ConfigurationManager.AppSettings["bootstrapServers"];
            string topic = ConfigurationManager.AppSettings["topic"];
            string saslConfig = ConfigurationManager.AppSettings["saslConfig"];
            string user = args[0];
            string password = args[1];
            string consumerGroupId = ConfigurationManager.AppSettings["consumerGroupId"];

            var config = new Dictionary<string, object>
            {
                { "group.id", consumerGroupId },
                { "bootstrap.servers", bootstrapList },
                // All of the following is for Bluemix Message Hub
                { "security.protocol", "SASL_SSL" },
                { "sasl.mechanisms", "PLAIN" },
                { "sasl.username", user },
                { "sasl.password", password },
                { "api.version.request", "true" },
                { "ssl.ca.location", @"DigicertGlobalRoot.cer" } // This is required to validate IBM's SSL certificate
            };

            using (var consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(topic);

                consumer.OnMessage += (s, msg) =>
                {
                    Console.WriteLine($"Partition: {msg.Partition} Offset: {msg.Offset} Key: {msg.Key} Value: {msg.Value}");
                };

                while (true)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}
