using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

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

            var queueName = "queue-order";
            var channel = CreateChannel(connection, queueName);
            
            BuildAndRunPublishers(channel, queueName, "Produtor A", manualResetEvent);
            manualResetEvent.WaitOne();
        }

        private static IModel CreateChannel(IConnection connection, string queueName)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName, false, false, false, null);
            
            return channel;
        }

        private static void BuildAndRunPublishers(IModel channel, string queue, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                var count = 0;
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir 100 msg");
                        Console.ReadLine();

                        for (var index = 0; index < 100; index++)
                        {
                            var message = $"OrderNumber: {count++} from {publisherName}";
                            var body = Encoding.UTF8.GetBytes(message);
                    
                            channel.BasicPublish("", queue, null, body);

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