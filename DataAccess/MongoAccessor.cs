using Accenture.DataSaver.Model;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;

namespace Accenture.DataSaver.DataAccess
{
    public class MongoAccessor
    {
        private readonly string _connectionString;
        private const string DatabaseName = "vavtar";

        public MongoAccessor(string connectionString)
        {
            this._connectionString = connectionString;
        }

        public MessageDto InsertResponse(MessageDto message)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collections = database.ListCollectionNames().ToList();
            var serviceIdentifier = message.operation;

            if (!collections.Any(x => x == serviceIdentifier))
            {
                database.CreateCollection(serviceIdentifier);
            }

            if (!collections.Any(x => x == "ServiceData"))
            {
                database.CreateCollection("ServiceData");
            }

            var serviceCollection = database.GetCollection<BsonDocument>("ServiceData");
            var filter_id = Builders<BsonDocument>.Filter.Eq("operation", message.operation);
            
            serviceCollection.ReplaceOne(filter_id,(new Metadata{operation = message.operation,
                    authenticationKey = message.authenticationKey,
                    authenticationMethod = message.authenticationMethod,
                    authenticationValue = message.authenticationValue,
                     }).ToBsonDocument(),new ReplaceOptions { IsUpsert = true });

            CreateIndexAsync("ServiceData", "operation");

            var collection = database.GetCollection<BsonDocument>(serviceIdentifier);
            var jdoc = (new MessageDto { response = message.response, request = new Body { formatted_data = message.request?.formatted_data, raw_data = message.request?.raw_data } }).ToBsonDocument();
            jdoc.Add("created_date", DateTime.Now);
            RemoveIdObject(jdoc);

            collection.InsertOne(jdoc);
            message.unique_id = jdoc["_id"].ToString();

            message.operation = serviceIdentifier;

            return message;
        }

        public MessageDto UpdateMessageRequest(MessageDto message)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collection = database.GetCollection<MessageDto>(message.operation);

            var filter_id = Builders<MessageDto>.Filter.Eq("_id", ObjectId.Parse(message.unique_id));

            var messageInDb = collection.FindSync(m => m._id == ObjectId.Parse(message.unique_id)).FirstOrDefault();
            if (messageInDb == null)
            {
                return null;
            }

            if (message?.request?.formatted_data != null && message.request.formatted_data.Count > 0)
            {
                if (messageInDb.request == null)
                {
                    messageInDb.request = new Body { formatted_data = message.request.formatted_data };
                }
            }

            if (message?.response?.raw_data != null)
            {
                if (messageInDb.response == null)
                {
                    messageInDb.response = new Body { raw_data = message?.response?.raw_data };
                }
            }

            collection.InsertOne(message);

            var result = collection.ReplaceOne(filter: filter_id, options: new ReplaceOptions { IsUpsert = true }, replacement: messageInDb);

            return message;
        }

        public string GetMapper(string operation)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collection = database.GetCollection<BsonDocument>("mappers");

            var response = collection.Find(filter: new BsonDocument("operation", operation), options: new FindOptions() { ShowRecordId = false }).FirstOrDefault();

            if (response != null)
            {
                BsonElement bsonElement;
                if (response.TryGetElement("_id", out bsonElement))
                    response.RemoveElement(bsonElement);
            }

            return response?.ToJson();
        }

        public string UpdateMapper(string dataObj)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collection = database.GetCollection<BsonDocument>("mappers");

            var bsonDocument = BsonSerializer.Deserialize<BsonDocument>(dataObj);

            var result = collection.ReplaceOne(filter: new BsonDocument("operation", bsonDocument["operation"].ToString()), options: new ReplaceOptions { IsUpsert = true }, replacement: bsonDocument);

            return result.UpsertedId.ToString();
        }

        public string UpdateRanker(string dataObj)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collection = database.GetCollection<BsonDocument>("rankers");
            CreateIndexAsync("rankers", "operation");



            var document = BsonSerializer.Deserialize<BsonDocument>(dataObj);
            RemoveIdObject(document);

            var result = collection.ReplaceOne(filter: new BsonDocument("operation", document["operation"].ToString()), options: new ReplaceOptions { IsUpsert = true }, replacement: document);

            return new JObject(new JProperty("result","Success")).ToString();

        }

        public string DeleteOperation(string operation)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            database.DropCollection(operation);

            var collection = database.GetCollection<BsonDocument>("rankers");

            var result = collection.DeleteOne(filter: new BsonDocument("operation", operation));

            var serviceDatacollection = database.GetCollection<BsonDocument>("ServiceData");
            collection.DeleteOne(filter: new BsonDocument("operation", operation));

            return new JObject(new JProperty("result",$"Success in deleting { result.DeletedCount } records")).ToString();

        }

        public string GetRanker(string operation)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetCollection<BsonDocument>("rankers");
            var servicecollection = database.GetCollection<BsonDocument>("ServiceData");

            var response = collection.Find(filter: new BsonDocument("operation", operation), options: new FindOptions() { ShowRecordId = false }).FirstOrDefault();

            var serviceMetadata = servicecollection.Find(filter: new BsonDocument("operation", operation), options: new FindOptions() { ShowRecordId = false }).FirstOrDefault();

            if(serviceMetadata != null){
                BsonElement bsonElement;
                if (serviceMetadata.TryGetElement("_id", out bsonElement))
                    serviceMetadata.RemoveElement(bsonElement);

            }

            if (response != null)
            {
                BsonElement bsonElement;
                if (response.TryGetElement("_id", out bsonElement))
                    response.RemoveElement(bsonElement);
                
                response.SetElement(new BsonElement("serviceData", serviceMetadata));

                return response.ToJson();
            }
            return new JObject(new JProperty("data", new JArray())).ToString();
        }

        private void CreateIndexAsync(string collectionName, string indexProperty)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);
            var indexKeysDefinition = Builders<BsonDocument>.IndexKeys.Ascending(indexProperty);
            collection.Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(indexKeysDefinition));
        }

       

        public string GetAllOperations()
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);
            var collection = database.GetCollection<BsonDocument>("rankers");

            var objects = collection.FindSync(filter: new BsonDocument()).ToList().Select(p => GetObject(p, "operation"));

            return objects.ToJson();
        }

        private static void RemoveIdObject(BsonDocument response)
        {
            BsonElement bsonElement;
            if (response.TryGetElement("_id", out bsonElement))
                response.RemoveElement(bsonElement);
        }

        private static string GetObject(BsonDocument elements, string name)
        {
            BsonElement bsonElement;
            if (elements.TryGetElement(name, out bsonElement))
                return bsonElement.Value?.ToString();

            return string.Empty;
        } 

        public string GetRequestFormattedData(string operation)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            if(database.ListCollectionNames().ToList().Any(x => x == operation))
            {
                var collection = database.GetCollection<BsonDocument>(operation);

                var objects = collection.FindSync(filter: new BsonDocument()).ToList();
                
                var jsonString = JsonConvert.SerializeObject(objects.ConvertAll(d => BsonTypeMapper.MapToDotNetValue(d)), Formatting.Indented);

                return jsonString;

            }

            return new JObject(new JProperty("request", new JObject(new JProperty("formatted_Data", new JArray())))).ToString();
        }

        public void DeleteResponse(string operation, string id)
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            if(database.ListCollectionNames().ToList().Any(x => x == operation))
            {
                var collection = database.GetCollection<BsonDocument>(operation);

                ObjectId objectId = ObjectId.Parse(id);
                var deleteFilter = Builders<BsonDocument>.Filter.Eq("_id", objectId);

                collection.DeleteOne(deleteFilter);
            }

        }

        public void LogOperation(string correlationId, string message, string exchange = "", string routingKey = "")
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            if(!database.ListCollectionNames().ToList().Any(x => x == "training_logs"))
            {
                database.CreateCollection("trianing_logs");
            }

            var collection = database.GetCollection<DataLog>("training_logs");

            collection.InsertOneAsync(new DataLog { CorrelationId = correlationId, Message = message, RoutingKey = routingKey, Exchange = exchange, Date = DateTime.Now });
        }

        public string GetAllLogs()
        {
            var client = new MongoClient(_connectionString);
            var database = client.GetDatabase(DatabaseName);

            var collection = database.GetCollection<DataLog>("training_logs");

            var findOptions = new FindOptions<DataLog>()
            {
                Limit = 200,
                Sort = Builders<DataLog>.Sort.Descending(x => x.Date)
            };

            findOptions.Projection = "{'_id': 0}";

            var objects = collection.FindSync(filter: new BsonDocument(), options: findOptions).ToList();

            var test = objects.GroupBy(x => x.Date.Date)
                .Select(y => new { Date = y.Key.ToShortDateString(), Count = y.Count(), Records = y.GroupBy(z => z.CorrelationId).Select(z => new { CorrelationId = z.Key, Count = z.Count(), Messages = z.ToList() }) });

            JObject rootData = new JObject();

            foreach(var dataLevel in objects.GroupBy(x => x.Date.ToString("MM/dd/yyyy")))
            {
                var requests = dataLevel.ToList().GroupBy(x => x.CorrelationId);
                var childRootData = new JObject();

                foreach(var req in requests)
                {
                    var messageRoot = new JObject();
                    foreach(var mroot in req.ToList())
                    {
                        messageRoot.Add(new JProperty(mroot.RoutingKey, new JObject(new JProperty("Exchange", mroot.Exchange), new JProperty("Message", JObject.Parse(mroot.Message)), new JProperty("Date", mroot.Date))));
                    }

                    childRootData.Add(new JProperty(req.Key, messageRoot));
                }

                rootData.Add(new JProperty(dataLevel.Key, childRootData));
            }

            return rootData.ToString();

        }
    }
}
