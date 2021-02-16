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

            var channel = SetupChannel(connection);
            
            BuildAndRunPublishers(channel,"Produtor A", manualResetEvent);
            manualResetEvent.WaitOne();
        }

        private static IModel SetupChannel(IConnection connection)
        {
            var channel = connection.CreateModel();
            
            // Criando diferentes queues
            channel.QueueDeclare(queue: "queue-order", false, false, false, null);
            channel.QueueDeclare(queue: "queue-logs", false, false, false, null);
            var ds = channel.QueueDeclare(queue: "queue-finance-orders", false, false, false, null);

            // Criação da exchange do tipo fanout
            channel.ExchangeDeclare("exchange-order", "fanout");
            
            // Associando as queues criadas ao novo exchange
            channel.QueueBind("queue-order", "exchange-order", "");
            channel.QueueBind("queue-logs", "exchange-order", "");
            channel.QueueBind("queue-finance-orders", "exchange-order", "");
            
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
                        Console.WriteLine("Pressione qualquer tecla para produzir 100 msg");
                        Console.ReadLine();

                        for (var index = 0; index < 100; index++)
                        {
                            var message = $"OrderNumber: {count++} from {publisherName}";
                            var body = Encoding.UTF8.GetBytes(message);
                            
                            // Invés de publicar direto na fila, será publicado no exchange e as mensagens serão copiadas para as filas associadas
                            // O Parâmetro 'routingKey' deixa de ser usado, agora deve ser configurado o 'exchange'
                            channel.BasicPublish("exchange-order", "", null, body);

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