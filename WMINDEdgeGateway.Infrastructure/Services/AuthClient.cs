using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using WMINDEdgeGateway.Application.DTOs;
using WMINDEdgeGateway.Application.Interfaces;
namespace WMINDEdgeGateway.Infrastructure.Services
{
    public class AuthClient : IAuthClient
    {
        private readonly HttpClient _http;

        public AuthClient(HttpClient http)
        {
            _http = http ?? throw new ArgumentNullException(nameof(http));
        }

        public async Task<AuthTokenResponse> GetTokenAsync(string clientId, string clientSecret)
        {
            if (string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(clientSecret))
                throw new ArgumentException("ClientId or ClientSecret cannot be empty");

            var form = new Dictionary<string, string>
            {
                { "client_id", clientId },
                { "client_secret", clientSecret }
            };

            var response = await _http.PostAsync("api/devices/connect/token", new FormUrlEncodedContent(form));

            var content = await response.Content.ReadAsStringAsync();
            Console.WriteLine($"Auth service status: {response.StatusCode}");
            Console.WriteLine($"Auth service response: {content}");

            response.EnsureSuccessStatusCode();

            var token = await response.Content.ReadFromJsonAsync<AuthTokenResponse>();

            if (token == null || string.IsNullOrEmpty(token.AccessToken))
                throw new Exception("Failed to retrieve a valid token from the auth service");

            return token;
        }
    }
}
