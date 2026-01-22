using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WMINDEdgeGateway.Domain.Entities
{
    public record DeviceConfiguration
 (
     Guid Id,
     string DeviceName,
     string ConfigurationJson
 );
}




