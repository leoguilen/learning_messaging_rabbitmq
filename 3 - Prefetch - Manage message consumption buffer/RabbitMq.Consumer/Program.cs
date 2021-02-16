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

            const string queueName = "queue-order";
            var channel = CreateChannel(connection, queueName);

            BuildAndRunWorker(channel: channel, "Worker A");
            BuildAndRunWorker(channel: channel, "Worker B");

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
            // Configuração do prefetch para balancear o consumo das mensagens em cada worker
            channel.BasicQos(0,1,false); // Parâmetro global = false - balanceamento per consumer / true - balanceamento per channel
            
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body.ToArray());
                    Console.WriteLine($"{channel.ChannelNumber} - {workerName}: [x] Received {message}");

                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
            };
            
            channel.BasicConsume(queue: "queue-order", autoAck: false, consumer: consumer);
        }
    }
}