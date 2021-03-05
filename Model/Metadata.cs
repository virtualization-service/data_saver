using MongoDB.Bson;
using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Accenture.DataSaver.Model
{
    public class Metadata
    {
        public string operation { get; set; }
        
        public string protocol { get; set; }

        public string soapaction { get; set; }

        public string authenticationMethod { get; set; }

        public string authenticationKey { get; set; }

        public string authenticationValue { get; set; }
    }
}