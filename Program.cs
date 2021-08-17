using System;
using System.Threading;
using Confluent.Kafka;
using KafkaConsumer.Models;

namespace KafkaConsumer
{
    class Program
    {
        static void Main()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var c = new ConsumerBuilder<long, byte[]>(conf)
                .Build();
            c.Subscribe("test");

            var deserializer = new PurchaseEventDeserializer();
            // Because Consume is a blocking call, we want to capture Ctrl+C and use a cancellation token to get out of our while loop and close the consumer gracefully.
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    // Consume a message from the test topic. Pass in a cancellation token so we can break out of our loop when Ctrl+C is pressed
                    var cr = c.Consume(cts.Token);
                    var obj = deserializer.Deserialize(cr.Message.Value, false, new SerializationContext());

                    Console.WriteLine($"Consumed message '{cr.Message.Value}' from topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}");
                    Console.WriteLine($"Object was {obj.ToString()} ");

                    // Do something interesting with the message you consumed
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                c.Close();
            }
        }
    }
}
