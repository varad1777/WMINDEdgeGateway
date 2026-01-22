using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace WMINDEdgeGateway.Infrastructure.Caching
{
    public class MemoryCacheService
    {
        private readonly MemoryCache _cache = new(new MemoryCacheOptions());

        public void Set<T>(string key, T value, TimeSpan ttl)
            => _cache.Set(key, value, ttl);

        public T? Get<T>(string key)
            => _cache.TryGetValue(key, out T value) ? value : default;
    }

}
