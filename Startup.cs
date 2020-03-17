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
                options.AddPolicy("AllowOrigin", builder => builder.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader());
            });

            services.AddSingleton<PublishMessage>();
            services.AddSingleton<MessageExtractor>();
            services.AddSingleton<MessageConsumer>();

            services.AddRabbitMQConnection(Configuration);

            var connectionString = Configuration.GetValue("connectionString", string.Empty);

            services.AddSingleton(m => new MongoAccessor(connectionString));

        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ConnectionFactory factory)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseCors();

            var processors = app.ApplicationServices.GetService<MessageConsumer>();
            var life = app.ApplicationServices.GetService<Microsoft.AspNetCore.Hosting.IApplicationLifetime>();
            life.ApplicationStarted.Register(GetOnStarted(factory, processors));
            life.ApplicationStopping.Register(GetOnStopped(factory, processors));

            app.UseMvc();
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
