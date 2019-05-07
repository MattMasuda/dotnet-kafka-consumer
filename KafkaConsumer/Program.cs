using Confluent.Kafka;
using System;
using System.Configuration;
using System.Threading;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var bootstrapList = ConfigurationManager.AppSettings["bootstrapServers"];
            var topic = ConfigurationManager.AppSettings["topic"];
            var consumerGroupId = ConfigurationManager.AppSettings["consumerGroupId"];

            var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;
            var config = new ConsumerConfig
            {
                GroupId = consumerGroupId,
                BootstrapServers = bootstrapList,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Uncomment the following lines for Message Hub
                //EnableAutoCommit = true,
                //SecurityProtocol = SecurityProtocol.SaslSsl,
                //SaslMechanism = SaslMechanism.Plain,
                //SaslUsername = args.Length > 0 ? args[0] : string.Empty,
                //SaslPassword = args.Length > 1 ? args[1] : string.Empty,
                //ApiVersionRequest = true,
                //SslCaLocation = @"DigicertGlobalRoot.cer"
            };

            using (var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) => Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]"))
                .SetPartitionsRevokedHandler((c, partitions) => Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]"))
                .Build())
            
            {
                Console.WriteLine($"Subscribing to topic {topic}");
                consumer.Subscribe(topic);
                // You can also subscribe to multiple topics with 
                // Subscribe(IEnumerable<string> topics)
                
                Consume(consumer, cancellationToken);
            }
        }

        private static void Consume(IConsumer<string, string> consumer, CancellationToken cancellationToken)
        {
            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Partition: {consumeResult.Partition} Offset: {consumeResult.Offset} Key: {consumeResult.Key} Value: {consumeResult.Value}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }
        }
    }
}
