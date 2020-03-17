using System;

namespace Accenture.DataSaver.Model
{
    public class DataLog
    {
        public string CorrelationId;
        public string Message;
        public string Exchange;
        public string RoutingKey;
        public DateTime Date;
    }
}
