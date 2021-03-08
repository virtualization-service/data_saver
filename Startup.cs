using Accenture.DataSaver.DataAccess;
using Accenture.DataSaver.Processors;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Steeltoe.CloudFoundry.Connector.RabbitMQ;
using System;

namespace Accenture.DataSaver
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddCors(options =>
           {
               options.AddDefaultPolicy(
                               builder =>
                               {
                                   builder.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
                               });
           });

            services.AddControllers();


            services.AddSingleton<PublishMessage>();
            services.AddSingleton<MessageExtractor>();
            services.AddSingleton<MessageConsumer>();

            services.AddRabbitMQConnection(Configuration);

            var connectionString = Configuration.GetValue("connectionString", string.Empty);
            
            if(! string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING")))
                connectionString = System.Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING");

            services.AddSingleton(m => new MongoAccessor(connectionString));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ConnectionFactory factory)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();
            app.UseCors();

            var processors = app.ApplicationServices.GetService<MessageConsumer>();
            var life = app.ApplicationServices.GetService<IHostApplicationLifetime>();
            life.ApplicationStarted.Register(GetOnStarted(factory, processors));
            life.ApplicationStopping.Register(GetOnStopped(factory, processors));

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }

        private static Action GetOnStarted(ConnectionFactory factory, MessageConsumer processors)
        {
            return () =>
            {
                processors.Register(factory);
            };
        }

        private static Action GetOnStopped(ConnectionFactory factory, MessageConsumer processors)
        {
            return () =>
            {
                processors.DeRegister(factory);
            };
        }
    }
}
