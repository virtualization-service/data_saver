using Accenture.DataSaver.DataAccess;
using Accenture.DataSaver.Model;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System;
using Microsoft.Extensions.Logging;

namespace Accenture.DataSaver.Processors
{
    public class MessageExtractor
    {
        PublishMessage _publisher;
        MongoAccessor _accessor;

        private readonly ILogger<MessageExtractor> _logger;


        public MessageExtractor(PublishMessage publisher, MongoAccessor accessor, ILogger<MessageExtractor> logger)
        {
            _logger = logger;
            _publisher = publisher;
            _accessor = accessor;
        }

        public void ConsumeMessage(BasicDeliverEventArgs message, ConnectionFactory connectionFactory)
        {
            var body = message.Body;
            var extactedMessage = Encoding.UTF8.GetString(body);
            _logger.LogInformation($"Received message with routing key {message.RoutingKey} exchange :{message.Exchange} & Body:{extactedMessage}");
            
            switch(message.RoutingKey)
            {
                case "configuration.train":
                    {
                        //var messageSaved = _accessor.InsertResponse(dataObject);
                        //_publisher.Publish(JsonConvert.SerializeObject(dataObject),connectionFactory);
                        break;
                    }
                case "parser.completed":
                    {
                        var dataObject = BsonSerializer.Deserialize<MessageDto>(extactedMessage);
                        var messageSaved = _accessor.InsertResponse(dataObject);
                        break;
                    }
                case "copier.completed":
                    {
                        _accessor.UpdateMapper(extactedMessage);
                        break;
                    }
                case "ranker.completed":
                    {
                        _accessor.UpdateRanker(extactedMessage);
                        break;
                    }

            }
        }
    }
}
