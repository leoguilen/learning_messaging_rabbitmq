using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

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
            
            // Habilitando o retorno de confirmação de recebimento das mensagens
            channel.ConfirmSelect();
            
            // Eventos com os resultados da publicação da mensagem
            channel.BasicAcks += ChannelOnBasicAcks;
            channel.BasicNacks += ChannelOnBasicNacks;
            channel.BasicReturn += ChannelOnBasicReturn;
            
            channel.QueueDeclare(queue: queueName, false, false, false, null);
            
            return channel;
        }

        private static void ChannelOnBasicNacks(object? sender, BasicNackEventArgs e)
        {
            Console.WriteLine($"{DateTime.Now:o} -> Basic Nack");
        }

        private static void ChannelOnBasicAcks(object? sender, BasicAckEventArgs e)
        {
            Console.WriteLine($"{DateTime.Now:o} -> Basic Ack");
        }

        private static void ChannelOnBasicReturn(object? sender, BasicReturnEventArgs e)
        {
            var msg = Encoding.UTF8.GetString(e.Body.ToArray());
            Console.WriteLine($"{DateTime.Now:o} -> Basic Return -> Original message -> {msg}");
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
                            var message = $"OrderNumber: {count++} from {publisherName}";
                            var body = Encoding.UTF8.GetBytes(message);
                    
                            channel.BasicPublish("", "yz", true, null, body);
                            
                            // Aguarda a confirmação das mensagens no tempo estipulado antes de dar timeout
                            channel.WaitForConfirms(TimeSpan.FromSeconds(10));
                            
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