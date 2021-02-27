using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMq.Consumer.Domain;

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

            var channel = CreateChannel(connection);

            BuildAndRunWorker(channel: channel, "Task worker", "task_queue");
            // BuildAndRunWorker(channel: channel, "Dead letter worker", "DeadLetterQueue");

            Console.ReadLine();
        }

        private static IModel CreateChannel(IConnection connection)
        {
            var channel = connection.CreateModel();
            
            // Criação da exchange e queue para mensagens que tiverem erros  
            channel.ExchangeDeclare("DeadLetterExchange", ExchangeType.Fanout);
            channel.QueueDeclare("DeadLetterQueue", true, false, false, null);
            channel.QueueBind("DeadLetterQueue", "DeadLetterExchange", "");
            
            // Esse argumento é setado no header para indicar as filas que enviaram suas mensagens com erros para a DeadLetterExchange
            var arguments = new Dictionary<string, object>()
            {
                {"x-dead-letter-exchange", "DeadLetterExchange"}
            };
            
            channel.QueueDeclare(queue: "task_queue", false, false, false, arguments: arguments);
            return channel;
        }
        
        public static void BuildAndRunWorker(IModel channel, string workerName, string queueName)
        {
            channel.BasicQos(0,1,false);
            
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body.ToArray());
                    
                    Console.WriteLine($"{channel.ChannelNumber} - {workerName} - {queueName} - {ea.RoutingKey}: [x] Received {message}");
                    var newOrder = JsonSerializer.Deserialize<Order>(message);
                    
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    channel.BasicNack(ea.DeliveryTag, false, false); // importante requeue ser falso
                }
            };
            
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
        }
    }
}