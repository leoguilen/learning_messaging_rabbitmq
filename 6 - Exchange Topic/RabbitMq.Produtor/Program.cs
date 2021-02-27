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
            
            channel.QueueDeclare(queue: "customers", false, false, false, null);
            channel.QueueDeclare(queue: "customers_sp", false, false, false, null);
            channel.QueueDeclare(queue: "customers_pr", false, false, false, null);

            channel.ExchangeDeclare("exchange-customer", "topic");

            channel.QueueBind("customers", "exchange-customer", "customers");
            channel.QueueBind("customers", "exchange-customer", "customers.*"); // * -> para fazer o binding de primeiro nivel
            //channel.QueueBind("customers", "exchange-customer", "customer.#"); // # -> para fazer o binding de todos os niveis
            channel.QueueBind("customers_sp", "exchange-customer", "customers.sp");
            channel.QueueBind("customers_pr", "exchange-customer", "customers.pr");
            
            return channel;
        }

        private static void BuildAndRunPublishers(IModel channel, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                var count = 0;
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir 10 msg");
                        Console.ReadLine();

                        for (var index = 0; index < 10; index++)
                        {
                            var message = $"OrderNumber: {count++} from {publisherName}";
                            var body = Encoding.UTF8.GetBytes(message);
                            
                            channel.BasicPublish("exchange-customer","customers", null, body);
                            channel.BasicPublish("exchange-customer","customers.sp", null, body);
                            channel.BasicPublish("exchange-customer","customers.pr", null, body);

                            Console.WriteLine($"{publisherName} - [x] Sent {count}", message);
                        }
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