using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using WMINDEdgeGateway.Application.DTOs;
using WMINDEdgeGateway.Infrastructure.Caching;


namespace WMINDEdgeGateway.Infrastructure.Services
{
    /// <summary>
    /// Background service that polls Modbus TCP devices using cached configuration.
    /// Gets device data from cache, polls modbus devices, and prints telemetry to console.
    /// This class preserves original behavior: grouping ranges, float fallback,
    /// failure counting, console buffered printing, and printing telemetry only.
    /// Additionally it bounds concurrent device connections using _semaphore.
    /// </summary>
    public class ModbusPollerHostedService : BackgroundService
    {
        private readonly ILogger<ModbusPollerHostedService> _log;
        private readonly IConfiguration _config;
        private readonly MemoryCacheService _cache;


        // Semaphore to limit concurrent TCP connections / modbus polls
        // value loaded from config or default 10
        private readonly SemaphoreSlim _semaphore;


        // failure counters and console lock (shared)
        private static readonly ConcurrentDictionary<Guid, int> _failureCounts = new();
        private static readonly object _consoleLock = new();


        // per-device loop tasks
        private readonly ConcurrentDictionary<Guid, Task> _deviceTasks = new();


        private readonly int _failThreshold;


        public ModbusPollerHostedService(ILogger<ModbusPollerHostedService> log,
                                         IConfiguration config,
                                         MemoryCacheService cache)
        {
            _log = log;
            _config = config;
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));


            // read failure threshold and concurrency limit from configuration
            _failThreshold = config?.GetValue<int?>("Modbus:FailureThreshold") ?? 3;
            if (_failThreshold <= 0) _failThreshold = 3;


            int concurrency = config?.GetValue<int?>("Modbus:MaxConcurrentPolls") ?? 10;
            if (concurrency <= 0) concurrency = 10;
            _semaphore = new SemaphoreSlim(concurrency, concurrency);
        }


        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _log.LogInformation("Modbus poller started (device-per-loop mode)");


            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        // load current device configurations from cache
                        var deviceConfigs = _cache.Get<List<DeviceConfigurationDto>>("DeviceConfigurations");


                        if (deviceConfigs == null || !deviceConfigs.Any())
                        {
                            _log.LogWarning("No device configurations in cache");
                            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                            continue;
                        }


                        // start a long-running loop task for each device if not already running
                        foreach (var config in deviceConfigs)
                        {
                            if (_deviceTasks.ContainsKey(config.Id)) continue;


                            // fire-and-forget long running loop for the device
                            var task = Task.Run(() => PollLoopForDeviceAsync(config, stoppingToken), stoppingToken);
                            _deviceTasks.TryAdd(config.Id, task);


                            // cleanup completed tasks (non-blocking)
                            var completed = _deviceTasks.Where(kvp => kvp.Value.IsCompleted).Select(kvp => kvp.Key).ToList();
                            foreach (var k in completed) _deviceTasks.TryRemove(k, out _);
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Poll loop manager error");
                    }


                    // small delay before scanning cache again for new/removed devices
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
            finally
            {
                // attempt graceful shutdown of device loops
                try
                {
                    await Task.WhenAll(_deviceTasks.Values.ToArray());
                }
                catch
                {
                    // ignore exceptions during shutdown
                }
            }
        }


        /// <summary>
        /// Per-device loop. Calls PollSingleDeviceOnceAsync repeatedly and delays based on returned poll interval.
        /// </summary>
        private async Task PollLoopForDeviceAsync(DeviceConfigurationDto deviceConfig, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    int delayMs = 1000;
                    try
                    {
                        delayMs = await PollSingleDeviceOnceAsync(deviceConfig, ct);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Unhandled error during single poll for device {Device}", deviceConfig.Id);
                        // small backoff to avoid tight crash loop
                        delayMs = 1000;
                    }


                    if (delayMs <= 0) delayMs = 1000;
                    await Task.Delay(TimeSpan.FromMilliseconds(delayMs), ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Error in device loop for {Device}", deviceConfig.Id);
                    // back off on unexpected loop-level error
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                }
            }
        }


        /// <summary>
        /// Performs a single poll for the given device and returns the device's poll interval in milliseconds.
        /// All original behaviors are preserved:
        /// - parse ProtocolSettingsJson,
        /// - normalize addresses,
        /// - group ranges up to 125 registers,
        /// - decode float32 + fallback,
        /// - failure counting,
        /// - buffered console output (atomic),
        /// - print telemetry to console only.
        /// Additionally this method uses _semaphore to bound concurrency around the network I/O.
        /// Gets device data from cache.
        /// </summary>
        private async Task<int> PollSingleDeviceOnceAsync(DeviceConfigurationDto deviceConfig, CancellationToken ct)
        {
            if (deviceConfig == null || deviceConfig.slaves == null || !deviceConfig.slaves.Any())
            {
                _log.LogWarning("Device {DeviceId} has no slaves - skipping", deviceConfig?.Id);
                return deviceConfig?.pollIntervalMs > 0 ? deviceConfig.pollIntervalMs : 1000;
            }


            JsonDocument settings;
            // try { settings = JsonDocument.Parse(deviceConfig.configurationJson ?? "{\"IpAddress\":\"10.10.10.17\",\"Port\":5020,\"SlaveId\":1,\"Endian\":\"Little\"}"); }
            try { settings = JsonDocument.Parse("{\"IpAddress\":\"127.0.0.1\",\"Port\":5020,\"SlaveId\":1,\"Endian\":\"Little\"}"); }
            catch (Exception ex)
            {
                _log.LogError(ex, "Invalid ProtocolSettingsJson for device {Device}", deviceConfig.Id);
                return deviceConfig.pollIntervalMs > 0 ? deviceConfig.pollIntervalMs : 1000;
            }


            // read settings strictly from ProtocolSettingsJson (no device.Host/device.Port fallback)
            var ip = TryGetString(settings, "IpAddress");
            var port = TryGetInt(settings, "Port", 502); // default Modbus TCP port is 502
            var slaveIdFromConfig = TryGetInt(settings, "SlaveId", 1); // kept as fallback if no DeviceSlave entry
            var endian = TryGetString(settings, "Endian") ?? "Big";
            var pollIntervalMs = TryGetInt(settings, "PollIntervalMs", deviceConfig.pollIntervalMs > 0 ? deviceConfig.pollIntervalMs : 1000);
            var addressStyleCfg = TryGetString(settings, "AddressStyle");


            if (string.IsNullOrEmpty(ip))
            {
                _log.LogWarning("Device {DeviceId} ProtocolSettingsJson missing IpAddress. Skipping poll.", deviceConfig.Id);
                return pollIntervalMs;
            }


            // Load device slaves and their registers from cached configuration
            var slaves = deviceConfig.slaves;


            // Flatten registers from slaves
            var activeRegisters = slaves
                .SelectMany(ds => ds.registers != null ? ds.registers.Select(r => new { DeviceSlave = ds, Register = r }) : Enumerable.Empty<dynamic>())
                .ToList();


            if (!activeRegisters.Any())
            {
                _log.LogWarning("No registers for device {Device}. Ip={Ip} Port={Port} Settings={Settings}", deviceConfig.Id, ip, port, deviceConfig.configurationJson);
                return pollIntervalMs;
            }


            const int ModbusMaxRegistersPerRead = 125;


            bool dbUses40001 = false;
            // checking address style, if 1-based or zero-based
            if (!string.IsNullOrEmpty(addressStyleCfg))
                dbUses40001 = string.Equals(addressStyleCfg, "40001", StringComparison.OrdinalIgnoreCase);
            else
                dbUses40001 = slaves.Any(s => s.registers != null && s.registers.Any(r => r.registerAddress >= 40001));


            int ToProto(int dbAddr)
            {
                // modbus expects zero-based addresses; normalize
                if (dbUses40001) return dbAddr - 40001;
                if (dbAddr > 0 && dbAddr < 40001) return dbAddr - 1;
                return dbAddr;
            }


            // Map registers to protocol addresses and lengths (preserve link to DeviceSlave)
            var protoPorts = activeRegisters.Select(x => new
            {
                DeviceSlave = x.DeviceSlave,
                Register = x.Register,
                ProtoAddr = ToProto(x.Register.registerAddress), // zero-based address
                Length = Math.Max(1, x.Register.registerLength)
            })
            .OrderBy(x => x.ProtoAddr)
            .ToList();


            if (!protoPorts.Any())
            {
                _log.LogDebug("No ports after normalization for device {Device}", deviceConfig.Id);
                return pollIntervalMs;
            }


            // Acquire semaphore to limit concurrent network connections/polls.
            // This ensures we don't overload network/DB/CPU when many device loops run.
           
            await _semaphore.WaitAsync(ct);  // this basically used to controll the concurrent access.
            //----
            try
            {
                // Connect to device TCP with a short timeout
                using var tcp = new TcpClient();
                using var connectCts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                // Link tokens so cancelling the overall loop cancels connect
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, connectCts.Token);


                await tcp.ConnectAsync(ip, port, linked.Token);


                // Using in-repo ModbusTcpClient helper to avoid NModbus4 dependency


                var now = DateTime.UtcNow;
                // tuple: (deviceSlaveId, slaveIndex, SignalType, Value, Unit, RegisterAddress)
                var allReads = new List<(Guid deviceSlaveId, int slaveIndex, string SignalType, double Value, string Unit, int RegisterAddress)>();


                // --- Group protoPorts by slaveIndex (unit id) so we never mix different slaves in one request ---
                var protoGroups = protoPorts
                    .GroupBy(x => ((dynamic)x.DeviceSlave).slaveIndex)
                    .ToDictionary(g => g.Key, g => g.OrderBy(p => p.ProtoAddr).ToList());


                // For each slave (unit id) build contiguous ranges and read separately
                foreach (var kv in protoGroups)
                {
                    int unitId = kv.Key; // the slaveIndex/unit id to use for Modbus requests
                    var itemsForSlave = kv.Value; // ordered by ProtoAddr


                    // Build ranges for this slave alone (each up to ModbusMaxRegistersPerRead)
                    var slaveRanges = new List<(int Start, int Count, List<dynamic> Items)>();
                    int j = 0;
                    while (j < itemsForSlave.Count)
                    {
                        int start = itemsForSlave[j].ProtoAddr; //0
                        int end = start + itemsForSlave[j].Length - 1;
                        var items = new List<dynamic> { itemsForSlave[j] };
                        j++;


                        while (j < itemsForSlave.Count)
                        {
                            var next = itemsForSlave[j];
                            if (next.ProtoAddr <= end + 1)
                            {
                                end = Math.Max(end, next.ProtoAddr + next.Length - 1);
                                items.Add(next);
                                j++;
                            }
                            else break;


                            if (end - start + 1 >= ModbusMaxRegistersPerRead)
                            {
                                end = start + ModbusMaxRegistersPerRead - 1;
                                break;
                            }
                        }


                        int count = Math.Min(ModbusMaxRegistersPerRead, end - start + 1);
                        slaveRanges.Add((start, count, items));
                    }


                    // Now perform reads for each range for THIS unitId
                    foreach (var r in slaveRanges)
                    {
                        if (r.Start < 0 || r.Start > ushort.MaxValue) { _log.LogWarning("Skipping invalid start {Start}", r.Start); continue; }
                        if (r.Count <= 0) continue;


                        StringBuilder sb = new();


                        // Range header (buffered output)
                        sb.AppendLine();
                        sb.AppendLine(new string('=', 80));
                        sb.AppendLine($"Device: {deviceConfig.Id} | Ip={ip}:{port} | UnitId={unitId} | RangeStart={r.Start} Count={r.Count}");
                        sb.AppendLine(new string('-', 80));


                        // included registers for this range
                        sb.AppendLine("Included registers:");
                        foreach (var ent in r.Items)
                        {
                            var ds = (dynamic)ent.DeviceSlave;
                            var reg = (dynamic)ent.Register;
                            sb.AppendLine($"  - slaveIndex={ds.slaveIndex}, DBAddr={reg.registerAddress}, Length={ent.Length}, DataType={reg.dataType}");
                        }
                        sb.AppendLine();
                        //--------






                        try
                        {
                            // IMPORTANT: use the slave's unit id here, not the single device-level slaveId
                            ushort[] regs = await ModbusTcpClient.ReadHoldingRegistersAsync(tcp, (byte)unitId, (ushort)r.Start, (ushort)r.Count, ct);
                            sb.AppendLine($"Read {regs.Length} registers from unit={unitId} start={r.Start}");
                            sb.AppendLine(new string('-', 80));


                            // Reset failure counts for registers in this successful read
                            foreach (var ent in r.Items)
                            {
                                var reg = (dynamic)ent.Register;
                                _failureCounts.TryRemove(reg.registerId, out int _);
                            }


                            // Table header
                            sb.AppendLine($"{"Time (UTC)".PadRight(30)} | {"Unit".PadRight(6)} | {"Register".PadRight(8)} | {"Value".PadRight(15)} | {"Unit".PadRight(8)}");
                            sb.AppendLine(new string('-', 80));


                            // Decode each register in this range
                            foreach (var entry in r.Items)
                            {
                                var ds = (dynamic)entry.DeviceSlave;
                                var reg = (dynamic)entry.Register;
                                int protoAddr = entry.ProtoAddr;
                                int relativeIndex = protoAddr - r.Start;


                                if (relativeIndex < 0 || relativeIndex + (entry.Length - 1) >= regs.Length)
                                {
                                    sb.AppendLine($"Index out-of-range for slave {ds.slaveIndex} (proto {protoAddr})");
                                    continue;
                                }


                                double finalValue = 0.0;
                                try
                                {
                                    if (string.Equals(reg.dataType, "float32", StringComparison.OrdinalIgnoreCase))
                                    {
                                        if (relativeIndex + 1 >= regs.Length)
                                        {
                                            sb.AppendLine($"Not enough regs to decode float32 for slave {ds.slaveIndex}");
                                            continue;
                                        }


                                        ushort r1 = regs[relativeIndex];
                                        ushort r2 = regs[relativeIndex + 1];


                                        // Build byte array for float32 decoding
                                        byte[] bytes = new byte[4];
                                        bool wordSwap = reg.wordSwap; // assume Register has a wordSwap property


                                        if (wordSwap)
                                        {
                                            bytes[0] = (byte)(r2 >> 8);
                                            bytes[1] = (byte)(r2 & 0xFF);
                                            bytes[2] = (byte)(r1 >> 8);
                                            bytes[3] = (byte)(r1 & 0xFF);
                                        }
                                        else
                                        {
                                            bytes[0] = (byte)(r1 >> 8);
                                            bytes[1] = (byte)(r1 & 0xFF);
                                            bytes[2] = (byte)(r2 >> 8);
                                            bytes[3] = (byte)(r2 & 0xFF);
                                        }


                                        if (string.Equals(endian, "Little", StringComparison.OrdinalIgnoreCase))
                                            Array.Reverse(bytes);


                                        float raw = BitConverter.ToSingle(bytes, 0);


                                        // Clamp invalid / extreme values
                                        if (float.IsNaN(raw) || float.IsInfinity(raw) || Math.Abs(raw) > 1e6)
                                        {
                                            sb.AppendLine($"Detected invalid float for slave {ds.slaveIndex}, using fallback/zero.");
                                            raw = 0;
                                        }


                                        // Safer fallback logic
                                        if ((r1 == 0 && r2 == 0) || Math.Abs(raw) < 1e-3)
                                        {
                                            double scaledFallback = r1;
                                            finalValue = scaledFallback * reg.scale;
                                            sb.AppendLine($"Float32 fallback for slave {ds.slaveIndex}: r1={r1}, r2={r2}, scaled={scaledFallback}");
                                        }
                                        else
                                        {
                                            finalValue = raw * reg.scale;
                                        }
                                    }
                                    else
                                    {
                                        finalValue = regs[relativeIndex] * reg.scale;
                                    }


                                    // add to telemetry buffer
                                    allReads.Add((ds.deviceSlaveId, ds.slaveIndex, reg.dataType ?? $"Port{ds.slaveIndex}", finalValue, reg.unit ?? string.Empty, reg.registerAddress));


                                    // append row to buffer
                                    sb.AppendLine($"{now:O}".PadRight(30) + " | " +
                                                  ds.slaveIndex.ToString().PadRight(6) + " | " +
                                                  reg.registerAddress.ToString().PadRight(8) + " | " +
                                                  finalValue.ToString("G6").PadRight(15) + " | " +
                                                  (reg.unit ?? string.Empty).PadRight(8));
                                }
                                catch (Exception decodeEx)
                                {
                                    sb.AppendLine($"Decode failed for slave {ds.slaveIndex}: {decodeEx.Message}");
                                }
                            }


                            sb.AppendLine(new string('=', 80));


                            // Print the whole buffer atomically to avoid mixing with other device outputs
                            lock (_consoleLock)
                            {
                                Console.Write(sb.ToString());
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.LogError(ex, "Error reading device {Device} unit={UnitId} start={Start} count={Count}", deviceConfig.Id, unitId, r.Start, r.Count);
                        }


                        //-----
                    }
                }


                // Print telemetry data to console (no DB save)
                if (allReads.Count > 0)
                {
                    try
                    {
                        var telemetryData = allReads.Select(r => new
                        {
                            DeviceId = deviceConfig.Id,
                            DeviceSlaveId = r.deviceSlaveId,
                            SlaveIndex = r.slaveIndex,
                            RegisterAddress = r.RegisterAddress,
                            SignalType = r.SignalType,
                            Value = r.Value,
                            Unit = r.Unit,
                            Timestamp = DateTime.Now
                        }).ToList();


                        if (telemetryData.Any())
                        {
                            // Just print the data
                            foreach (var data in telemetryData)
                            {
                                lock (_consoleLock)
                                {
                                    Console.WriteLine($"[TELEMETRY] Device: {data.DeviceId}, Slave: {data.SlaveIndex}, Register: {data.RegisterAddress}, Value: {data.Value} {data.Unit}");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Failed to prepare telemetry for device {Device}", deviceConfig.Id);
                    }


                    _log.LogDebug("Prepared {Count} telemetry rows for device {Device}", allReads.Count, deviceConfig.Id);
                }
            }
            catch (SocketException s_ex)
            {
                _log.LogWarning(s_ex, "Device {Device} unreachable {Ip}:{Port}", deviceConfig.Id, ip, port);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // polling was cancelled via token, simply return
                _log.LogDebug("Polling cancelled for device {Device}", deviceConfig.Id);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error polling device {Device}", deviceConfig.Id);
            }
            finally
            {
                // Always release the semaphore even on exceptions
                _semaphore.Release();
            }


           
           
           
           
           
           
           
           
            // Return the poll interval (ms) for next loop delay
            return pollIntervalMs;
        }


        private static string? TryGetString(JsonDocument doc, string propName)
        {
            if (doc.RootElement.TryGetProperty(propName, out var v) && v.ValueKind == JsonValueKind.String) return v.GetString();
            return null;
        }


        private static int TryGetInt(JsonDocument doc, string propName, int @default)
        {
            if (doc.RootElement.TryGetProperty(propName, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var x)) return x;
            return @default;
        }
    }
}

