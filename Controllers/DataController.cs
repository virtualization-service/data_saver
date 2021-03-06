﻿using Accenture.DataSaver.DataAccess;
using Accenture.DataSaver.Model;
using Accenture.DataSaver.Processors;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Accenture.DataSaver.Controllers
{
    [Route("api/[controller]")]
    //[EnableCors("AllowOrigin")]
    [ApiController]
    public class DataController : ControllerBase
    {
        private readonly MongoAccessor _accessor;
        private readonly PublishMessage _publisher;

        public DataController(MongoAccessor accessor, PublishMessage publisher)
        {
            _accessor = accessor;
            _publisher = publisher;
        }

        [HttpGet("ranker")]
        public ActionResult GetRanker(string operation)
        {
            return Ok(_accessor.GetRanker(operation));
        }

        [HttpGet("mapper")]
        public ActionResult GetMapper([FromQuery] string operation)
        {
            return Ok(_accessor.GetMapper(operation));
        }

        [HttpGet("operations")]
        public ActionResult GetAllOperations()
        {
            return Ok(_accessor.GetAllOperations());
        }

        [HttpDelete("operation")]
        public ActionResult<string> DeleteOperation([FromQuery] string operation, [FromServices] ConnectionFactory connection)
        {
            var messageSaved = _accessor.DeleteOperation(operation);

            return Ok(messageSaved);
        }

        [HttpGet("responses")]
        public ActionResult GetRequestFormattedData([FromQuery] string operation)
        {
            return Ok(_accessor.GetRequestFormattedData(operation));
        }

        [HttpDelete("responses")]
        public ActionResult GetRequestFormattedData([FromQuery] string operation, [FromQuery] string id)
        {
            _accessor.DeleteResponse(operation, id);
            return Ok("Success");
        }

        [HttpPost]
        public ActionResult<string> POST([FromBody] MessageDto message, [FromServices] ConnectionFactory connection)
        {
            if (message?.service == null || message?.request?.raw_data == null || message?.response?.raw_data == null)
                return BadRequest("Request Missing Mandatory data");

            if (message?.service == null)
                return BadRequest("Request misssing Mandatory Data");

            _publisher.Publish(JsonConvert.SerializeObject(message), connection);

            return Ok(message);
        }

        [HttpPost("ranker")]
        public ActionResult<string> UdpateRanker([FromBody] object message, [FromServices] ConnectionFactory connection)
        {
            var messageSaved = _accessor.UpdateRanker(message.ToString());

            return Ok(messageSaved);
        }

        [HttpPost("recordNew")]
        public ActionResult<string> RecordNewOperation([FromBody] object message, [FromServices] ConnectionFactory connection)
        {
            return Ok(_accessor.RecordNewOperation(message.ToString()));
        }

        [HttpPost("recordExisting")]
        public ActionResult<string> RecordExistingOperation([FromBody] object message, [FromServices] ConnectionFactory connection)
        {
            return Ok(_accessor.RecordExistingOperation(message.ToString()));
        }

        [HttpGet("recordOperations")]
        public ActionResult GetAllRecordOperations()
        {
            return Ok(_accessor.GetAllRecordLearningOperations());
        }

        [HttpGet("activityLogs")]
        public ActionResult GetAllLogs()
        {
            return Ok(_accessor.GetAllLogs());
        }


        public ActionResult<string> SaveMessage([FromBody] Object message)
        {
            var json = JObject.Parse(message.ToString());
            var dataObject = BsonSerializer.Deserialize<MessageDto>(json.ToString());
            var test = JsonConvert.DeserializeObject<MessageDto>(message.ToString());
            var messageSaved = _accessor.InsertResponse(test);

            return Ok(json.ToString());
        }

    }
}
