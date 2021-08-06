using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using CloudKarafka.Pocos.Net;
using System.Text.Json;


namespace CloudKarafka.Consumer.Net
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

            var config = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.ScramSha256,
                SaslUsername = username,
                SaslPassword = password,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            using var c = new ConsumerBuilder<Ignore, string>(config).Build();
            c.Subscribe(topic);

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                for (; ; )
                {
                    try
                    {
                        var cr = await Task.FromResult(c.Consume(cts.Token));
                        var weatherRequestJson = cr.Message.Value;
                        var weatherRequest = JsonSerializer.Deserialize<WeatherRequest>(weatherRequestJson);
                        Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                        Console.WriteLine(weatherRequest.Location);
                        
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }
}
