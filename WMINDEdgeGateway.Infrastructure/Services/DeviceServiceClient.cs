using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading.Tasks;
using WMINDEdgeGateway.Application.DTOs;
using WMINDEdgeGateway.Application.Interfaces;

namespace WMINDEdgeGateway.Infrastructure.Services
{
    public class DeviceServiceClient : IDeviceServiceClient
    {
        private readonly HttpClient _http;
        private readonly TokenService _tokenService;

        public DeviceServiceClient(HttpClient http, TokenService tokenService)
        {
            _http = http ?? throw new ArgumentNullException(nameof(http));
            _tokenService = tokenService ?? throw new ArgumentNullException(nameof(tokenService));
        }

        public async Task<DeviceConfigurationDto[]> GetConfigurationsAsync(string gatewayId)
        {
            if (string.IsNullOrWhiteSpace(gatewayId))
                throw new ArgumentException("Gateway ID cannot be empty", nameof(gatewayId));

            async Task<HttpResponseMessage> SendRequest()
            {
                var token = await _tokenService.GetTokenAsync();

                _http.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);

                return await _http.GetAsync($"/api/devices/devices/configurations/gateway/{gatewayId}");
            }

            // First call
            var response = await SendRequest();

            // If token expired → refresh & retry once
            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                Console.WriteLine("401 Unauthorized → Refreshing token & retrying...");

                response = await SendRequest();
            }

            response.EnsureSuccessStatusCode();

            var apiResponse = await response.Content
                .ReadFromJsonAsync<ApiResponse<DeviceConfigurationDto[]>>();

            if (apiResponse == null)
                return Array.Empty<DeviceConfigurationDto>();

            if (!apiResponse.Success)
                throw new Exception($"API Error: {apiResponse.Error}");

            return apiResponse.Data ?? Array.Empty<DeviceConfigurationDto>();
        }
    }
}
