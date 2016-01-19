namespace Deploy
{
    using System.Collections.Generic;

    using Newtonsoft.Json;

    public class DeploymentConfig
    {
        public List<ApplicationDeploymentConfig> Applications { get; set; }

        [JsonIgnore]
        public string ETag { get; set; }
    }
}