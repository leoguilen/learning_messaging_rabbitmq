using System;

namespace RabbitMq.Produtor.Domain
{
    public class Order
    {
        public Guid Id { get; }
        public DateTime CreateDate { get; }
        public DateTime LastUpdated { get; private set; }
        public long Amount { get; private set; }
        public long TransactionKey { get; private set; }
        
        public Order(long amount, long transactionKey)
        {
            Id = Guid.NewGuid();
            Amount = amount;
            TransactionKey = transactionKey;
            CreateDate = DateTime.Now;
        }

        public void UpdateOrder(long amount)
        {
            Amount = amount;
            LastUpdated = DateTime.Now;
        }
    }
}