using System;
using System.Text.Json;
using Confluent.Kafka;

namespace KafkaConsumer.Models
{
    public class PurchaseEvent
    {
        public long Id { get; set; }
        public DateTime TimeStamp { get; set; }
        public string ItemPurchased { get; set; }
        public float Price { get; set; }
        public override string ToString() => $"Id: {Id.ToString()}, Item: {ItemPurchased}, Price: {Price.ToString()}, TS: {TimeStamp.ToString()}";
    }
    public class PurchaseEventDeserializer : IDeserializer<PurchaseEvent>
    {
        public PurchaseEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonSerializer.Deserialize<PurchaseEvent>(data);
        }
    }
}

