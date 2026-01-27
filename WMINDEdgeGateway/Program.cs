using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Net.Http;
using WMINDEdgeGateway.Application.DTOs;
using WMINDEdgeGateway.Application.Interfaces;
using WMINDEdgeGateway.Infrastructure.Caching;
using WMINDEdgeGateway.Infrastructure.Services;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // -----------------------------
        // EXISTING DEPENDENCIES
        // -----------------------------
        services.AddSingleton<IAuthClient>(sp =>
        {
            var http = new HttpClient { BaseAddress = new Uri("http://localhost:5000") };
            return new AuthClient(http);
        });

        services.AddSingleton<IDeviceServiceClient>(sp =>
        {
            var http = new HttpClient { BaseAddress = new Uri("http://localhost:5000") };
            return new DeviceServiceClient(http);
        });

        services.AddSingleton<MemoryCacheService>();
        services.AddMemoryCache();

        // -----------------------------
        // OPC UA BACKGROUND SERVICE
        // -----------------------------
        services.AddHostedService<OpcUaPollerHostedService>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .Build();

// -----------------------------
// INITIALIZE CACHE BEFORE STARTING
// -----------------------------
await InitializeCacheAsync(host.Services);

// -----------------------------
// START THE HOST
// -----------------------------
await host.RunAsync();

// -----------------------------
// HELPER METHOD
// -----------------------------
async Task InitializeCacheAsync(IServiceProvider services)
{
    var authClient = services.GetRequiredService<IAuthClient>();
    var deviceClient = services.GetRequiredService<IDeviceServiceClient>();
    var cache = services.GetRequiredService<MemoryCacheService>();

    Console.WriteLine("Edge Gateway Console App Starting...");

    string gatewayClientId = "GW-11c4f00a40204babb2a62796f1616b35";
    string gatewayClientSecret = "SohYn6CiMdkwubukjv0XCnSm24qVNHGl1T3uMT0v3xg=";

    // Fetch device configs from API
    var token = (await authClient.GetTokenAsync(gatewayClientId, gatewayClientSecret))?.AccessToken ?? "";
    var configs = await deviceClient.GetConfigurationsAsync(gatewayClientId, token);

    // -----------------------------
    // CREATE OPC UA TEST DEVICE
    // -----------------------------
    var opcDevice = new DeviceConfigurationDto(
        Id: Guid.NewGuid(),
        DeviceName: "Machine1",
        Protocol: "OpcUa",
        PollIntervalMs: 1000,
        ConfigurationJson: "{}",
        Slaves: new[]
        {
            new DeviceSlaveDto(
                DeviceSlaveId: Guid.NewGuid(),
                SlaveIndex: 1,
                IsHealthy: true,
                Registers: null,
                Signals: new[]
                {
                    new OpcSignalDto(
                        SignalId: Guid.NewGuid(),
                        SignalName: "Temperature",
                        NodeId: "ns=2;s=WMIND/Machine1/Temperature",
                        DataType: "Double",
                        Scale: 1.0,
                        Unit: "°C",
                        IsHealthy: true
                    ),
                    new OpcSignalDto(
                        SignalId: Guid.NewGuid(),
                        SignalName: "Voltage",
                        NodeId: "ns=2;s=WMIND/Machine1/Voltage",
                        DataType: "Double",
                        Scale: 1.0,
                        Unit: "V",
                        IsHealthy: true
                    )
                }
            )
        }
    );

    // Combine all configs and set in cache
    var allConfigs = (configs ?? Array.Empty<DeviceConfigurationDto>()).ToList();
    allConfigs.Add(opcDevice);
    
    cache.Set("DeviceConfigurations", allConfigs, TimeSpan.FromMinutes(30));
    cache.PrintCache();

    Console.WriteLine("✅ OPC UA Test Device Injected into Cache");
    Console.WriteLine($"✅ Total devices in cache: {allConfigs.Count}");
}