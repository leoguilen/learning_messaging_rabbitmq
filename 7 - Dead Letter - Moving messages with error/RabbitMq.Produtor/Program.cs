using System;
using System.Collections.Generic;
using System.Text;
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
            
            // Esse argumento é setado no header para indicar as filas que enviaram suas mensagens com erros para a DeadLetterExchange
            var arguments = new Dictionary<string, object>()
            {
                {"x-dead-letter-exchange", "DeadLetterExchange"}
            };
            
            channel.QueueDeclare(queue: "task_queue", false, false, false, arguments);
            return channel;
        }

        private static void BuildAndRunPublishers(IModel channel, string publisherName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Pressione qualquer tecla para produzir 10 msg");
                        Console.ReadLine();

                        for (var index = 0; index < 10; index++)
                        {
                            var order = new Order(1000, new Random().Next(100,200));
                            var message = order.ToString();
                            
                            // Mensagem inválida para gerar erro no consumidor
                            if (index % 3 == 0)
                            {
                                message = "Não é uma ordem válida";
                            }
                            
                            var body = Encoding.UTF8.GetBytes(message);
                            
                            channel.BasicPublish("","task_queue", null, body);

                            Console.WriteLine($"{publisherName} - [x] Sent {index}: {message}");
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