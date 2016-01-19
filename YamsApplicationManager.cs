namespace Deploy
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    using Newtonsoft.Json;

    /// <summary>
    /// Utility for synchronizing local directories with Azure Blob Storage for use with YAMS.
    /// </summary>
    public class YamsApplicationManager
    {
        private const int MaxAttempts = 6;

        private const int RetryWait = 15;

        private const string DeploymentConfigFileName = "DeploymentConfig.json";

        private readonly CloudBlobContainer container;

        private static readonly BlobRequestOptions BlobRequestOptions = new BlobRequestOptions
        {
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };

        public YamsApplicationManager(string storageConnectionString)
        {
            // Retrieve storage account from connection string.
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client 
            var client = storageAccount.CreateCloudBlobClient();
            this.container = client.GetContainerReference("applications");
        }

        public async Task<DeploymentConfig> GetDeploymentConfig(CancellationToken cancellationToken)
        {
            var blob = this.container.GetBlockBlobReference(DeploymentConfigFileName);
            try
            {
                var appSpecJson = await blob.DownloadTextAsync(Encoding.UTF8, null, null, null, cancellationToken);
                var result = JsonConvert.DeserializeObject<DeploymentConfig>(appSpecJson);
                result.ETag = blob.Properties.ETag;
                return result;
            }
            catch (StorageException ex)
            {
                var statusCode = ((ex.InnerException as WebException)?.Response as HttpWebResponse)?.StatusCode;
                if (statusCode == null || statusCode != HttpStatusCode.NotFound)
                {
                    throw;
                }

                Console.WriteLine($"Warning, DeploymentConfig.json not found at {blob.Uri}, returning default configuration.");
                return new DeploymentConfig { Applications = new List<ApplicationDeploymentConfig>() };
            }
        }

        public async Task UploadDeploymentConfig(DeploymentConfig deploymentConfig, CancellationToken cancellationToken)
        {
            var accessCondition = AccessCondition.GenerateIfMatchCondition(deploymentConfig.ETag);
            var blob = this.container.GetBlockBlobReference(DeploymentConfigFileName);
            await
                blob.UploadTextAsync(
                    JsonConvert.SerializeObject(deploymentConfig),
                    Encoding.UTF8,
                    accessCondition,
                    null,
                    null,
                    cancellationToken);
        }

        /// <summary>
        /// Lists versions of the specified application in sorted order.
        /// </summary>
        /// <param name="appName">
        /// The application name.
        /// </param>
        /// <param name="cancellationToken">
        /// The token.
        /// </param>
        /// <returns>
        /// A list versions.
        /// </returns>
        public async Task<List<Version>> GetVersions(string appName, CancellationToken cancellationToken)
        {
            var results = await this.GetVersionedDirectories(appName, cancellationToken);
            return results.Select(_ => _.Key).ToList();
        }

        /// <summary>
        /// Returns <see langword="true"/> if the contents of <paramref name="localDirectory"/> match the contents of the remote blob directory with the specified <paramref name="remoteVersion"/>, <see langword="false"/> otherwise.
        /// </summary>
        /// <param name="appName">The application name</param>
        /// <param name="localDirectory">The local directory.</param>
        /// <param name="remoteVersion">The remote version</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        public async Task<bool> ContentsMatch(
            string appName,
            string localDirectory,
            Version remoteVersion,
            CancellationToken cancellationToken)
        {
            var remoteDirectory = this.container.GetDirectoryReference(GetDirectoryName(appName, remoteVersion));
            var existingFileChecksums
                = await GetBlobDirectoryChecksums(remoteDirectory, cancellationToken);
            
            // Compare local and remote directories.
            var localFileCheckSums = ComputeLocalChecksums(localDirectory);
            var diff = DiffDirectories(localFileCheckSums, existingFileChecksums);
            return diff.Modified.Count == 0;
        }

        public struct BlobFileInfo
        {
            public BlobFileInfo(string name, string hash, CloudBlobDirectory directory)
            {
                this.Name = name;
                this.Hash = hash;
                this.Directory = directory;
            }
            
            public string Name { get; }
            public string Hash { get; }
            public CloudBlobDirectory Directory { get;  }
        }

        /// <summary>
        /// Uploads the given folder.
        /// </summary>
        /// <param name="application">
        /// The application to deploy.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <param name="otherAppNames">
        /// Names of other applications which remote files can be copied from.
        /// </param>
        /// <returns>
        /// A <see cref="Task"/> representing the work performed.
        /// </returns>
        public async Task UploadApplication(
            ApplicationMapping application,
            CancellationToken cancellationToken,
            List<string> otherAppNames = null)
        {
            var previousVersionChecksums = await this.GetPreviousVersionChecksums(application.Id, cancellationToken);

            // If other app names were specified, also check those applications for files.
            if (otherAppNames != null)
            {
                foreach (var otherAppName in otherAppNames)
                {
                    var otherAppVersionChecksums = await this.GetPreviousVersionChecksums(otherAppName, cancellationToken);
                    foreach (var file in otherAppVersionChecksums)
                    {
                        List<BlobFileInfo> files;
                        if (!previousVersionChecksums.TryGetValue(file.Key, out files))
                        {
                            previousVersionChecksums.Add(file.Key, file.Value);
                        }
                        else
                        {
                            files.AddRange(file.Value);
                        }
                    }
                }
            }

            // Compare with existing checksums
            var localFileChecksums = ComputeLocalChecksums(application.SourceDirectory);
            var diff = DiffDirectories(localFileChecksums, previousVersionChecksums);

            // Copy unchanged files from the existing container to the new container.
            await
                ForEachParallel(
                    diff.Unchanged,
                    file =>
                    this.UploadFile(
                        application.Id,
                        application.Version,
                        file.Name,
                        true,
                        application.SourceDirectory,
                        file.Directory,
                        cancellationToken));

            var attempts = 0;
            var previousCount = diff.Modified.Count;
            while (diff.Modified.Count > 0)
            {
                // Upload modified files to the new containerprocessor =
                await
                    ForEachParallel(
                        diff.Modified,
                        file =>
                        this.UploadFile(application.Id, application.Version, file, false, application.SourceDirectory, null, cancellationToken));

                var targetDirectory = this.container.GetDirectoryReference(GetDirectoryName(application.Id, application.Version));
                var updatedCheckSums = await GetBlobDirectoryChecksums(targetDirectory, cancellationToken);
                localFileChecksums = ComputeLocalChecksums(application.SourceDirectory);
                diff = DiffDirectories(localFileChecksums, updatedCheckSums);

                // Wait prior to a retry
                if (diff.Modified.Count > 0)
                {
                    await Task.Delay(RetryWait, cancellationToken);
                }

                // Break the leases of any outstanding files.
                if (previousCount == diff.Modified.Count)
                {
                    attempts++;
                }
                previousCount = diff.Modified.Count;
                if (attempts == MaxAttempts)
                {
                    foreach (var file in diff.Modified)
                    {
                        await this.BreakLease(file, cancellationToken);
                    }
                    attempts = 0;
                }
            }

            // Keep only the two latest versions.
            await this.DeleteOldVersions(application.Id, cancellationToken);
        }

        /// <summary>
        /// Deletes old versions of the specified application, leaving only the current version and the previous version.
        /// </summary>
        /// <param name="appName">The application name.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task DeleteOldVersions(string appName, CancellationToken cancellationToken)
        {
            var directories = await this.GetVersionedDirectories(appName, cancellationToken);
            if (directories.Count > 2)
            {
                // Leave only the newest version and the previous version.
                foreach (var directory in directories.Take(directories.Count - 2))
                {
                    BlobContinuationToken continuationToken = null;
                    do
                    {
                        var blobs =
                            await
                            directory.Value.ListBlobsSegmentedAsync(
                                true,
                                BlobListingDetails.Metadata,
                                100,
                                continuationToken,
                                null,
                                null,
                                cancellationToken);

                        continuationToken = blobs.ContinuationToken;
                        foreach (var blob in blobs.Results.OfType<CloudBlob>())
                        {
                            await blob.DeleteAsync(cancellationToken);
                        }
                    }
                    while (continuationToken != null);
                }
            }
        }

        private async Task<Dictionary<string, List<BlobFileInfo>>> GetPreviousVersionChecksums(string appName, CancellationToken cancellationToken)
        {
            // Check if there are any existing containers
            var directories = await this.GetVersionedDirectories(appName, cancellationToken);
            var previousVersionExists = directories.Count > 0;

            Dictionary<string, List<BlobFileInfo>> previousVersionChecksums;
            if (previousVersionExists)
            {
                // Get the existing checksums
                var previousVersionDirectory = directories.LastOrDefault().Value;
                previousVersionChecksums = await GetBlobDirectoryChecksums(previousVersionDirectory, cancellationToken);
            }
            else
            {
                return new Dictionary<string, List<BlobFileInfo>>();
            }

            return previousVersionChecksums;
        }

        private static async Task ForEachParallel<T>(IEnumerable<T> collection, Func<T, Task> action)
        {
            var processor = new ActionBlock<T>(
                action,
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 64, SingleProducerConstrained = true });
            foreach (var file in collection)
            {
                processor.Post(file);
            }

            processor.Complete();
            await processor.Completion;
        }

        /// <summary>
        /// Returns the checksums in the specified folder.
        /// </summary>
        /// <param name="root">
        /// The directory to retrieve checksums for.
        /// </param>
        /// <param name="prefixLength">
        /// The length of the prefix to trim from the start of the path.
        /// </param>
        /// <returns>
        /// The checksums in the specified folder.
        /// </returns>
        private static Dictionary<string, string> ComputeLocalChecksums(string root, int prefixLength)
        {
            // Recurse into each subdirectory.
            var directory = Directory.GetDirectories(root);
            var checksums = directory.SelectMany(_ => ComputeLocalChecksums(_, prefixLength))
                .ToDictionary(_ => _.Key, _ => _.Value);

            // Add hashes for all the files in the current directory.
            var files = Directory.GetFiles(root);
            using (var md5 = new MD5CryptoServiceProvider())
            {
                foreach (var file in files)
                {
                    using (var stream = new FileStream(file, FileMode.Open, FileAccess.Read))
                    {
                        var hash = Convert.ToBase64String(md5.ComputeHash(stream));
                        var relativePath = file.Substring(prefixLength).Replace('\\', '/');
                        checksums.Add(relativePath, hash);
                    }
                }
            }

            return checksums;
        }

        /// <summary>
        /// Recursively compute checksums in the given folder.
        /// </summary>
        /// <param name="root">
        /// The root directory.
        /// </param>
        /// <returns>
        /// A dictionary of file-md5 key value pairs.
        /// </returns>
        private static Dictionary<string, string> ComputeLocalChecksums(string root)
        {
            return ComputeLocalChecksums(root, root.Length + 1);
        }

        /// <summary>
        /// Get the checksums for the given container.
        /// </summary>
        /// <param name="directory">
        /// The container.
        /// </param>
        /// <param name="token">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// A dictionary of file-md5 key value pairs.
        /// </returns>
        private static async Task<Dictionary<string, List<BlobFileInfo>>> GetBlobDirectoryChecksums(
            CloudBlobDirectory directory,
            CancellationToken token)
        {
            var blobChecksums = new Dictionary<string, List<BlobFileInfo>>();
            BlobContinuationToken continuationToken = null;
            do
            {
                var result =
                    await
                    directory.ListBlobsSegmentedAsync(
                        true,
                        BlobListingDetails.Metadata,
                        100,
                        continuationToken,
                        null,
                        null,
                        token);

                var blobs = result.Results.OfType<CloudBlob>();
                foreach (var blob in blobs)
                {
                    var key = blob.Name.Substring(directory.Prefix.Length);
                    List<BlobFileInfo> files;
                    if (!blobChecksums.TryGetValue(key, out files))
                    {
                        files = new List<BlobFileInfo>();
                        blobChecksums.Add(key, files);
                    }

                    files.Add(new BlobFileInfo(key, blob.Properties.ContentMD5, directory));
                }
                continuationToken = result.ContinuationToken;
            }
            while (continuationToken != null);

            return blobChecksums;
        }

        /// <summary>
        /// Returns the diff between the two file-md5 dictionaries.
        /// </summary>
        /// <param name="newFiles">
        /// The first set of file dictionaries.
        /// </param>
        /// <param name="existingFiles">
        /// The second set of file dictionaries.
        /// </param>
        /// <returns>
        /// A directory diff containing the modified and unchanged files.
        /// </returns>
        private static DirectoryDiff DiffDirectories(
            Dictionary<string, string> newFiles,
            Dictionary<string, List<BlobFileInfo>> existingFiles)
        {
            var newKeys = newFiles.Keys;
            var existingKeys = existingFiles.Keys;

            var common = newKeys.Intersect(existingKeys);
            var modified = newKeys.Except(existingKeys).ToList();
            var unchanged = new List<BlobFileInfo>();
            foreach (var key in common)
            {
                List<BlobFileInfo> existing;
                var currentHash = newFiles[key];
                existingFiles.TryGetValue(key, out existing);
                if (existing == null)
                {
                    // This file is new.
                    modified.Add(key);
                    continue;
                }

                // Check if the file exists with the same hash.
                var existingFile = existing.FirstOrDefault(_ => currentHash  == _.Hash);
                if (string.Equals(currentHash, existingFile.Hash, StringComparison.Ordinal))
                {
                    // The file exists remotely.
                    unchanged.Add(existingFile);
                }
                else
                {
                    // The file differ from all remote copies..
                    modified.Add(key);
                }
            }

            return new DirectoryDiff(modified, unchanged);
        }

        private static Version GetVersion(string appName, CloudBlobDirectory directory)
        {
            var versionSegment = directory.Prefix.Substring(appName.Length + 1).TrimEnd('/');
            Version version;

            Version.TryParse(versionSegment, out version);
            return version;
        }

        /// <summary>
        /// The comparison function for versions.
        /// </summary>
        /// <returns>
        /// 1 if the second arg is greater
        /// 0 if the args are equal
        /// -1 if the first arg is greater
        /// </returns>
        private static int VersionComparison(
            KeyValuePair<Version, CloudBlobDirectory> first,
            KeyValuePair<Version, CloudBlobDirectory> second)
        {
            var firstVersion = first.Key;
            var secondVersion = second.Key;
            if (firstVersion != null && secondVersion != null)
            {
                return firstVersion.CompareTo(secondVersion);
            }

            // If both are null, use ordinal comparison of strings.
            if (firstVersion == null && secondVersion == null)
            {
                return 0;
            }

            // If first is null, second is greater.
            if (firstVersion == null)
            {
                return 1;
            }

            // If second is null, first is greater.
            return -1;
        }

        private async Task BreakLease(string blobName, CancellationToken token)
        {
            try
            {
                var blob = this.container.GetBlobReference(blobName);
                await blob.BreakLeaseAsync(TimeSpan.FromSeconds(2), token);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"Error trying to break lease for '{blobName}': {exception.ToDetailedString()}");
            }
        }

        /// <summary>
        /// Creates a file in the new container.
        /// </summary>
        /// <param name="version">
        /// The app version.
        /// </param>
        /// <param name="file">
        /// The blob name.
        /// </param>
        /// <param name="unchanged">
        /// True if the blob is being copied from a previous container.
        /// </param>
        /// <param name="rootDirectory">
        /// The root directory prefix.
        /// </param>
        /// <param name="previousVersionDirectory">
        /// THe previous container.
        /// </param>
        /// <param name="token">
        /// The cancellation token.
        /// </param>
        /// <param name="appName">
        /// The app name.
        /// </param>
        /// <returns>
        /// A <see cref="Task"/> representing the work performed.
        /// </returns>
        private async Task UploadFile(
            string appName,
            Version version,
            string file,
            bool unchanged,
            string rootDirectory,
            CloudBlobDirectory previousVersionDirectory,
            CancellationToken token)
        {
            var targetBlobName = GetBlobName(appName, version, file);
            var blob = this.container.GetBlockBlobReference(targetBlobName);
            try
            {
                if (unchanged)
                {
                    var previousBlob = previousVersionDirectory.GetBlockBlobReference(file);
                    try
                    {
                        Console.WriteLine(
                            $"Copying {file} from blob {previousBlob.Name}.");
                        await blob.StartCopyAsync(previousBlob, token);
                        Console.WriteLine(
                            $"Copied {file} from blob {previousBlob.Name}.");
                        return;
                    }
                    catch (Exception ex)
                    {
                        // If there's an error copying it, upload it instead.
                        Console.WriteLine($"Copy of blob {targetBlobName} from {previousBlob.Uri} failed: {ex.ToDetailedString()}");
                    }
                }

                var localFile = file.Replace('/', Path.DirectorySeparatorChar);
                var fullPath = Path.Combine(rootDirectory, localFile);
                Console.WriteLine($"Uploading {file} from file {localFile}.");
                await blob.UploadFromFileAsync(fullPath, FileMode.Open, null, BlobRequestOptions, null, token);
                Console.WriteLine($"Uploaded {file} from file {localFile}.");
            }
            catch (StorageException ex)
            {
                var statusCode = ((ex.InnerException as WebException)?.Response as HttpWebResponse)?.StatusCode;
                if (statusCode == null || statusCode != HttpStatusCode.Conflict)
                {
                    throw;
                }

                Console.WriteLine($"Conflict when uploading {targetBlobName}. Ignoring.");
            }
        }

        private static string GetBlobName(string appName, Version newVersion, string blobName)
        {
            return $"{GetDirectoryName(appName, newVersion)}{blobName}";
        }

        private static string GetDirectoryName(string appName, Version version)
        {
            return $"{appName}/{version.Major}.{version.Minor}.{version.Build}/";
        }

        /// <summary>
        /// Lists versions of the specified application in sorted order.
        /// </summary>
        /// <param name="appName">
        /// The application name.
        /// </param>
        /// <param name="cancellationToken">
        /// The token.
        /// </param>
        /// <returns>
        /// A list versions.
        /// </returns>
        private async Task<List<KeyValuePair<Version, CloudBlobDirectory>>> GetVersionedDirectories(
            string appName,
            CancellationToken cancellationToken)
        {
            var containerList = new List<KeyValuePair<Version, CloudBlobDirectory>>();
            BlobContinuationToken continuationToken = null;
            var prefix = appName.TrimEnd('/') + "/";
            do
            {
                var result = await this.container.ListBlobsSegmentedAsync(prefix, continuationToken, cancellationToken);
                containerList.AddRange(
                    result.Results.OfType<CloudBlobDirectory>()
                        .Select(_ => new KeyValuePair<Version, CloudBlobDirectory>(GetVersion(appName, _), _)));
                continuationToken = result.ContinuationToken;
            }
            while (continuationToken != null);

            containerList.Sort(VersionComparison);

            return containerList;
        }

        /// <summary>
        /// The directory diff class.
        /// </summary>
        public class DirectoryDiff
        {
            /// <summary>
            /// THe list of modified files.
            /// </summary>
            public List<string> Modified { get; }

            /// <summary>
            /// The list of unchanged files.
            /// </summary>
            public IEnumerable<BlobFileInfo> Unchanged { get; }

            /// <summary>
            /// The constructor.
            /// </summary>
            /// <param name="modified">The list of modifed files.</param>
            /// <param name="unchanged">The list of unchanged files.</param>
            public DirectoryDiff(List<string> modified, IEnumerable<BlobFileInfo> unchanged)
            {
                this.Modified = modified;
                this.Unchanged = unchanged;
            }
        }
    }
}