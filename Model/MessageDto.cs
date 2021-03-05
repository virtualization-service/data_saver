using MongoDB.Bson;
using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Accenture.DataSaver.Model
{
    public class MessageDto
    {
        public string unique_id;
        public ObjectId? _id;
        public Uri service;
        public string operation;

        public string protocol{ get; set;}

        public string soapaction{ get; set;}

        public string authenticationMethod{ get; set;}

        public string authenticationKey{ get; set;}

        public string authenticationValue{ get; set;}

        public Body request { get; set; }
        public Body response { get; set; }

        public Dictionary<string, string[]> request_response_mapper { get; set; }

        [JsonIgnore]
        public string service_component{get;set;}
    }

    public class Body
    {
        public string raw_data { get; set; }
        public Dictionary<string, string[]> formatted_data;
        public Dictionary<string, string> headers;
    }

    public class FlattenedData
    {
        public string path;
        public string[] values;
    }
}
