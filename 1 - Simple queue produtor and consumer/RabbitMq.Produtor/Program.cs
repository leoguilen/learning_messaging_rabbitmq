using System;
using System.Text;
using System.Threading;
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
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "qconsoleapp",
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            var count = 0;
            while (true)
            {
                var message = $"{count++} Hello world by rabbitmq";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "",
                    routingKey: "qconsoleapp",
                    basicProperties: null,
                    body: body);
                Console.WriteLine(" [x] Sent {0}", message);
                Thread.Sleep(500);
            }
        }
    }
}