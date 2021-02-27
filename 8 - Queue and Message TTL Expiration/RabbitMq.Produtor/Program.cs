using System;
using System.Collections.Generic;
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

            var queueName = "test_time_to_live";
            var channel = CreateChannel(connection, queueName);
            
            BuildAndRunPublishers(channel, queueName, "Produtor A", manualResetEvent);
            manualResetEvent.WaitOne();
        }

        private static IModel CreateChannel(IConnection connection, string queueName)
        {
            var channel = connection.CreateModel();
            
            // Configurando tempo de expiração de mensagens de uma queue
            // ou seja, após o tempo todas mensagens dessa queue serão deletadas
            var args = new Dictionary<string, object>()
            {
                {"x-message-ttl",20000}
            };
            
            channel.QueueDeclare(queue: queueName, false, false, false, args);
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
                        Console.WriteLine("Pressione qualquer tecla para produzir 10 msg");
                        Console.ReadLine();

                        for (var index = 0; index < 10; index++)
                        {
                            var message = $"OrderNumber: {count++} from {publisherName} at {DateTime.Now}";
                            var body = Encoding.UTF8.GetBytes(message);

                            // Configurando tempo de expiração da mensagem enviada
                            // ou seja, após o tempo a mensagem será deletada
                            var props = channel.CreateBasicProperties();
                            props.Expiration = "10000"; // esse tempo é ms
                            
                            channel.BasicPublish("", queue, null, body);

                            Console.WriteLine($"{publisherName} - [x] Sent {count}: {message}");
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