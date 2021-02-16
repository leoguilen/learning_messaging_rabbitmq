using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMq.Consumer
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

            var queueName = "queue-order";
            var channel = CreateChannel(connection, queueName);

            BuildAndRunWorker(channel: channel, "Worker A");
            Console.ReadLine();
        }

        private static IModel CreateChannel(IConnection connection, string queueName)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare(queue: queueName, false, false, false, null);
            
            return channel;
        }
        
        public static void BuildAndRunWorker(IModel channel, string workerName)
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body.ToArray());
                    Console.WriteLine($"{channel.ChannelNumber} - {workerName}: [x] Received {message}");

                    // Confirmação manual de consumo da mensagem
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    // Não confirmação de consumo da mensagem e retornando para fila 
                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
            };
            
            // Confirmação automática de consumo de mensagens
            // channel.BasicConsume(queue: "queue-order", autoAck: true, consumer: consumer);
            
            // O ideal é manter parâmetro 'autoAck' como falso
            channel.BasicConsume(queue: "queue-order", autoAck: false, consumer: consumer);
        }
    }
}