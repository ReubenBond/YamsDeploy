namespace Deploy
{
    using System.Collections.Generic;

    public class ApplicationDeploymentConfig
    {
        public string Id { get; set; }
        public string Version { get; set; }
        public List<string> DeploymentIds { get; set; }
    }
}