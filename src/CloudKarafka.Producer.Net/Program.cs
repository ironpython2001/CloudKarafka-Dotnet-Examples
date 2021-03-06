using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;
using System.Threading.Tasks;
using CloudKarafka.Pocos.Net;
using System.Text.Json;

namespace CloudKarafka.Producer.Net
{
    class Program
    {
        public static IConfigurationRoot configuration;
        static async Task Main(string[] args)
        {
            configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetParent(AppContext.BaseDirectory).FullName)
            .AddJsonFile("appsettings.json", false)
            .Build();

            IServiceCollection serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton<IConfigurationRoot>(configuration);

            var bootstrapServers = configuration["BootstrapServers"].ToString();
            var username = configuration["UserName"].ToString();
            var password = configuration["Password"].ToString();
            var topicPrifix = configuration["TopicPrefix"].ToString();
            var topic = $"{topicPrifix}default";

            var config = new ProducerConfig()
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = username,
                SaslPassword = password,
            };

            using var p = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                var weatherRequest = new WeatherRequest() { Location = "hyderabad" };
                var weatherRequestJson = JsonSerializer.Serialize(weatherRequest);
                var dr = await p.ProduceAsync($"{topic}", new Message<Null, string>
                {
                    //Value = $"topic posted on {DateTime.Now}"
                    Value= weatherRequestJson
                });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
