using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
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
        IConfiguration configuration = context.Configuration;

        // -----------------------------
        // AUTH CLIENT
        // -----------------------------
        services.AddSingleton<IAuthClient>(sp =>
        {
            var http = new HttpClient
            {
                BaseAddress = new Uri(configuration["Services:AuthBaseUrl"]!)
            };
            return new AuthClient(http);
        });

        // TOKEN SERVICE (ADD THIS)
        // -----------------------------
        services.AddSingleton<TokenService>(sp =>
        {
            var authClient = sp.GetRequiredService<IAuthClient>();
            var memoryCache = sp.GetRequiredService<IMemoryCache>();
            var clientId = configuration["Gateway:ClientId"]!;
            var clientSecret = configuration["Gateway:ClientSecret"]!;
            return new TokenService(authClient, memoryCache, clientId, clientSecret);
        });

        // -----------------------------
        // DEVICE SERVICE CLIENT
        // -----------------------------
        services.AddSingleton<IDeviceServiceClient>(sp =>
        {
            var http = new HttpClient
            {
                BaseAddress = new Uri(configuration["Services:DeviceApiBaseUrl"]!)
            };
            return new DeviceServiceClient(http);
        });

        // -----------------------------
        // CACHE
        // -----------------------------
        services.AddMemoryCache();
        services.AddSingleton<MemoryCacheService>();

        // -----------------------------
        // MODBUS BACKGROUND SERVICE
        // -----------------------------
        services.AddHostedService<ModbusPollerHostedService>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    })
    .Build();

// ---------------------------------
// INITIALIZE CACHE BEFORE HOST START
// ---------------------------------
await InitializeCacheAsync(host.Services);

// ---------------------------------
// START HOST
// ---------------------------------
await host.RunAsync();

// ---------------------------------
// HELPER METHOD
// ---------------------------------
async Task InitializeCacheAsync(IServiceProvider services)
{
    var tokenService = services.GetRequiredService<TokenService>();
    var deviceClient = services.GetRequiredService<IDeviceServiceClient>();
    var cache = services.GetRequiredService<MemoryCacheService>();
    var configuration = services.GetRequiredService<IConfiguration>();

    Console.WriteLine("Edge Gateway Console App Starting...");

    string gatewayClientId = configuration["Gateway:ClientId"]!;

    // ✅ Get auto-refresh token
    var accessToken = await tokenService.GetTokenAsync();

    var configs = await deviceClient.GetConfigurationsAsync(gatewayClientId, accessToken);
    var allConfigs = (configs ?? Array.Empty<DeviceConfigurationDto>()).ToList();

    cache.Set("DeviceConfigurations", allConfigs, TimeSpan.FromMinutes(30));
    cache.PrintCache();

    Console.WriteLine("Modbus devices loaded into cache");
    Console.WriteLine($"Total devices in cache: {allConfigs.Count}");
}

