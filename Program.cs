namespace Deploy
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    using Humanizer;

    using Newtonsoft.Json;

    using PowerArgs;
    
    [ArgExceptionBehavior(ArgExceptionPolicy.StandardExceptionHandling)]
    public class Arguments
    {
        [ArgDescription("The DeploymentId of the Yams cluster."), ArgRequired]
        public string DeploymentId { get; set; }

        [ArgDescription("The Azure Storage connection string"), ArgRequired]
        public string ConnectionString { get; set; }

        [ArgDescription("The version to upload.")]
        public string Version { get; set; }

        [ArgDescription("The application identifier.")]
        public string Id { get; set; }

        [ArgDescription("The local directory containing the application."), ArgExistingDirectory]
        public string SourceDirectory { get; set; }

        [ArgDescription("Whether or not to read application mapping from console input. If true, Id, SourceDirectory, & Version are ignored."), ArgDefaultValue(false)]
        public bool StdIn { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            ServicePointManager.DefaultConnectionLimit = Environment.ProcessorCount * 8;
            ServicePointManager.Expect100Continue = false;

            try
            {
                var parsedArgs = Args.Parse<Arguments>(args);
                var deploymentMapping = GetApplicationMapping(parsedArgs);
                Run(deploymentMapping, parsedArgs.DeploymentId, parsedArgs.ConnectionString).Wait();
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.ToDetailedString());
            }
        }

        private static async Task Run(IEnumerable<ApplicationMapping> applications, string deploymentId, string storageConnectionString)
        {
            var stopwatch = Stopwatch.StartNew();
            Console.WriteLine($"Environment: {deploymentId}");
            var manager = new YamsApplicationManager(storageConnectionString);
            var cancellationToken = CancellationToken.None;

            var deploymentConfig = await manager.GetDeploymentConfig(cancellationToken);
            var updated = false;
            foreach (var application in applications)
            {
                // Set a suitable version for the application.
                var versions = await manager.GetVersions(application.Id, cancellationToken);
                var previousVersion = versions.LastOrDefault();
                application.Version = application.Version ?? IncrementVersion(previousVersion) ?? new Version(1, 0, 0);

                // Check if the application needs to be uploaded, and upload it if so.
                if (previousVersion != null
                    && await
                       manager.ContentsMatch(application.Id, application.SourceDirectory, previousVersion, cancellationToken))
                {
                    Console.WriteLine(
                        $"Local copy of application {application.Id} is identical to remote version v{previousVersion}, skipping upload.");
                    application.Version = previousVersion;
                }
                else
                {
                    Console.WriteLine($"Will upload {application.Id} v{application.Version} from {application.SourceDirectory}.");

                    var otherAppNames =
                        deploymentConfig.Applications.Select(_ => _.Id).Where(_ => _ != application.Id).ToList();
                    await
                        manager.UploadApplication(
                            application,
                            cancellationToken,
                            otherAppNames);
                    
                    Console.WriteLine($"Uploaded {application.Id} v{application.Version}.");
                }
                
                // Update the deployment config.
                var found = false;
                var updatedVersionString = application.Version?.ToString();
                foreach (var app in deploymentConfig.Applications)
                {
                    if (!string.Equals(app.Id, application.Id, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    // Update the version.
                    if (!string.Equals(app.Version, updatedVersionString, StringComparison.Ordinal))
                    {
                        app.Version = updatedVersionString;
                        updated = true;
                    }

                    // Add the deployment id.
                    if (app.DeploymentIds == null)
                    {
                        app.DeploymentIds = new List<string> { deploymentId };
                        updated = true;
                    }
                    else if (!app.DeploymentIds.Contains(deploymentId))
                    {
                        app.DeploymentIds.Add(deploymentId);
                        updated = true;
                    }

                    found = true;
                    break;
                }

                // If the application did not exist in the deployment config, add it.
                if (!found)
                {
                    deploymentConfig.Applications.Add(
                        new ApplicationDeploymentConfig
                        {
                            DeploymentIds = new List<string> { deploymentId },
                            Id = application.Id,
                            Version = updatedVersionString
                        });
                    updated = true;
                }
            }

            if (updated)
            {
                Console.WriteLine("Updating deployment configuration.");
                await manager.UploadDeploymentConfig(deploymentConfig, cancellationToken);
                Console.WriteLine($"Deployment complete in {stopwatch.Elapsed.Humanize()}.");
            }
            else
            {
                Console.WriteLine($"Deployment complete in {stopwatch.Elapsed.Humanize()}. No applications updated.");
            }
        }

        private static Version IncrementVersion(Version previosuVersion)
        {
            var version = default(Version);
            if (previosuVersion != null)
            {
                version = new Version(previosuVersion.Major, previosuVersion.Minor, previosuVersion.Build + 1);
            }

            return version;
        }

        private static List<ApplicationMapping> GetApplicationMapping(Arguments args)
        {
            List<ApplicationMapping> applicationMapping;
            if (args.StdIn)
            {
                if (!string.IsNullOrWhiteSpace(args.SourceDirectory))
                {
                    throw new ArgumentException(
                        $"{nameof(Arguments.SourceDirectory)} cannot be specified if {nameof(Arguments.StdIn)} is specified.");
                }

                if (!string.IsNullOrWhiteSpace(args.Version))
                {
                    throw new ArgumentException(
                        $"{nameof(Arguments.Version)} cannot be specified if {nameof(Arguments.StdIn)} is specified.");
                }

                if (!string.IsNullOrWhiteSpace(args.Id))
                {
                    throw new ArgumentException(
                        $"{nameof(Arguments.Id)} cannot be specified if {nameof(Arguments.StdIn)} is specified.");
                }
                
                applicationMapping = JsonConvert.DeserializeObject<List<ApplicationMapping>>(Console.In.ReadToEnd());
            }
            else
            {
                applicationMapping = new List<ApplicationMapping>
                {
                    new ApplicationMapping
                    {
                        Id = args.Id,
                        SourceDirectory = args.SourceDirectory,
                        Version = new Version(args.Version)
                    }
                };
            }

            return applicationMapping;
        }
    }
}
