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

        public DeviceServiceClient(HttpClient http)
        {
            _http = http ?? throw new ArgumentNullException(nameof(http));
        }

        public async Task<DeviceConfigurationDto[]> GetConfigurationsAsync(string gatewayId, string token)
        {
            if (string.IsNullOrWhiteSpace(gatewayId))
                throw new ArgumentException("Gateway ID cannot be empty", nameof(gatewayId));

            if (string.IsNullOrWhiteSpace(token))
                throw new ArgumentException("Token cannot be empty", nameof(token));

            // Clear previous headers and add Bearer token
            _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            // Call API
            var response = await _http.GetAsync($"/api/devices/devices/configurations/gateway/{gatewayId}");
            response.EnsureSuccessStatusCode();

            // Deserialize JSON using the wrapper
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<DeviceConfigurationDto[]>>();

            if (apiResponse == null)
                return Array.Empty<DeviceConfigurationDto>();

            if (!apiResponse.Success)
                throw new Exception($"API returned error: {apiResponse.Error}");

            return apiResponse.Data ?? Array.Empty<DeviceConfigurationDto>();
        }
    }
}
