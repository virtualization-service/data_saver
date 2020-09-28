using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;

namespace Accenture.DataSaver.Processors
{
    public class MessageConsumer
    {

        MessageExtractor _extractor;

        private IModel _channel;

        private readonly ILogger<MessageConsumer> _logger;

        public MessageConsumer(MessageExtractor extractor, ILogger<MessageConsumer> logger)
        {
            _logger=logger;
            _extractor = extractor;
        }

        public void Register(ConnectionFactory factory)
        {
            var connection = factory.CreateConnection();

            _channel = connection.CreateModel();

            _channel.ExchangeDeclare("configuration", type: "topic", durable: true);
            _channel.QueueDeclare("dataSaver");
            _channel.QueueBind("dataSaver", "configuration", "*.*");


            _channel.ConfirmSelect();

            _channel.BasicAcks += ChannelBasicAck;

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (consumerModel, ea) =>
            {
                try
                {
                    _extractor.ConsumeMessage(ea, factory);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error {ex}");
                }
            };

            _channel.BasicQos(0, 10000, false);
            _channel.BasicConsume("dataSaver", true, consumer: consumer);
        }

        public void DeRegister(ConnectionFactory factory)
        {
            if (_channel != null) _channel.Close();
        }

        private void ChannelBasicAck(object sender, BasicAckEventArgs e)
        {
            Console.WriteLine($"Received Acknowledgement {e.DeliveryTag}");
        }
    }
}
