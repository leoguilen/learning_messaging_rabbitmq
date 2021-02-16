using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMq.Produtor.Domain;

namespace RabbitMq.Produtor
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost", 
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };
            var manualResetEvent = new ManualResetEvent(false);

            manualResetEvent.Reset();

            using var connection = factory.CreateConnection();

            var channel = SetupChannel(connection);
            
            BuildAndRunPublishers(channel,"Produtor A", manualResetEvent);
            manualResetEvent.WaitOne();
        }

        private static IModel SetupChannel(IConnection connection)
        {
            var channel = connection.CreateModel();
            
            channel.QueueDeclare(queue: "queue-order", false, false, false, null);
            channel.QueueDeclare(queue: "queue-finance-orders", false, false, false, null);

            channel.ExchangeDeclare("exchange-order", "direct");
            
            // Usando o parâmetro 'routingKey' para definir a chave/evento que será usado para inserir uma mensagem nessa fila 
            channel.QueueBind("queue-order", "exchange-order", "order_new");
            channel.QueueBind("queue-order", "exchange-order", "order_upd");
            channel.QueueBind("queue-finance-orders", "exchange-order", "order_new");
            
            return channel;
        }

        private static void BuildAndRunPublishers(IModel channel, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                var keyIndex = 1;
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir uma order");
                        Console.ReadLine();

                        var order = new Order(new Random().Next(1000,9999),keyIndex++);
                        var message = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));
                        
                        channel.BasicPublish("exchange-order","order_new", null, message);
                        Console.WriteLine($"{order.TransactionKey} > New order id {order.Id}: Amount {order.Amount} | Created: {order.CreateDate:o}");
                        
                        order.UpdateOrder(new Random().Next(1000,9999));
                        message = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));
                        
                        channel.BasicPublish("exchange-order","order_upd", null, message);
                        Console.WriteLine($"{order.TransactionKey} > Upd order id {order.Id}: Amount {order.Amount} | LastUpdated: {order.LastUpdated:o}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        manualResetEvent.Set();
                    }
                }
            });
        }
    }
}