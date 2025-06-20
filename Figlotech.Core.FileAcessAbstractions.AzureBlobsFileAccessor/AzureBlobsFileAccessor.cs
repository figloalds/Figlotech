using Figlotech.Core.Interfaces;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public sealed class BlobMetaData {
        public string Relative;
        public long Length;
        public DateTime LastWrite;
    }

    public sealed class BlobsCredentials {
        public string AccountName { get; set; }
        public string AccountKey { get; set; }

        public static BlobsCredentials Annonymous(String accountName) {
            return new BlobsCredentials { AccountName = accountName };
        }
    }

    public sealed class AzureBlobsFileAccessor : IFileSystem {

        private static int gid = 0;
        private static int myid = ++gid;

        public bool IsCaseSensitive => false;

        String AccountName { get; set; }
        String AccountKey { get; set; }
        String ContainerName { get; set; }
        bool IsReadOnly { get; set; } = true;
        CloudBlobContainer BlobContainer;

        public void InitBlobFileAccessor(string host, string accountName, string containerName, string accountKey) {

            AccountName = accountName;
            AccountKey = accountKey;
            ContainerName = containerName;

            CloudBlobClient cloudBlobClient;

            IsReadOnly = AccountKey == null;
            
            if (!IsReadOnly) {
                string connectionString =
                    (host != null ?
                        //"UseDevelopmentStorage=true;" +
                        $"BlobEndpoint=http://{host}/{accountName};"
                        :
                        "DefaultEndpointsProtocol=https;"
                    ) +
                    (accountName != null && AccountKey != null ? $"AccountName={accountName};AccountKey={accountKey};" : "");

                CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
                cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            } else {
                var uri = new Uri($@"https://{AccountName}.blob.core.windows.net");
                cloudBlobClient = new CloudBlobClient(uri);
            }
            BlobContainer = cloudBlobClient.GetContainerReference(containerName);

            //if (!IsReadOnly) {
            //    BlobContainer.CreateIfNotExistsAsync().Wait();
            //}
        }

        string BaseDirectory { get; set; } = "";

        public void SetBaseDirectory(string path) {
            BaseDirectory = path.Replace(Path.DirectorySeparatorChar, '/');
            if(!BaseDirectory.EndsWith("/")) {
                BaseDirectory += '/';
            }
        }

        public AzureBlobsFileAccessor(string host, BlobsCredentials credentials, string containerName) {
            InitBlobFileAccessor(host, credentials?.AccountName, containerName, credentials?.AccountKey);
        }
        public AzureBlobsFileAccessor(BlobsCredentials credentials, string containerName) {
            InitBlobFileAccessor(null, credentials?.AccountName, containerName, credentials?.AccountKey);
        }

        private void AbsMkDirs(string dir) {
            // Blobs are kinda crazy, folders don't exist there, so there's no need for this
        }

        public async Task MkDirsAsync(string dir) {
            AbsMkDirs(dir);
        }

        public IFileSystem Fork(string relative) {             
            FixRelative(ref relative);
            var newAccessor = new AzureBlobsFileAccessor(AccountName, new BlobsCredentials { AccountKey = AccountKey }, ContainerName);
            newAccessor.SetBaseDirectory(BaseDirectory + relative.Replace(Path.DirectorySeparatorChar, '/'));
            return newAccessor;
        }

        string FixRelative(ref string relative) {
            if (relative.StartsWith("/")) {
                relative = relative.Substring(1);
            }
            relative = BaseDirectory + relative.Replace(Path.DirectorySeparatorChar, '/');
            return relative;
        }

        public async Task Write(String relative, Func<Stream, Task> func) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            using (var stream = blob.OpenWriteAsync().Result) {
                await func(stream);
            }
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        private void RenDir(string relative, string newName) {
            
        }

        public async Task RenameAsync(string relative, string newName) {
            FixRelative(ref relative);
            FixRelative(ref newName);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            CloudBlockBlob newBlob = BlobContainer.GetBlockBlobReference(newName);

            await newBlob.StartCopyAsync(blob).ConfigureAwait(false);
            await blob.DeleteAsync().ConfigureAwait(false);
        }

        public async Task<DateTime?> GetLastModifiedAsync(string relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            if (!await blob.ExistsAsync().ConfigureAwait(false)) {
                return DateTime.MinValue;
            }
            if (!blob.Properties.LastModified.HasValue || blob.Properties.LastModified == DateTime.MinValue) {
                await blob.FetchAttributesAsync().ConfigureAwait(false);
            }
            var dt = blob.Properties.LastModified ?? DateTime.MinValue;
            return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc);
        }
        public async Task<DateTime?> GetLastAccessAsync(string relative) {
            FixRelative(ref relative);
            return await GetLastModifiedAsync(relative).ConfigureAwait(false);
        }

        public async Task<long> GetSizeAsync(string relative) {
            FixRelative(ref relative);
            CloudBlob blob = BlobContainer.GetBlobReference(relative);
            if (!await blob.ExistsAsync().ConfigureAwait(false)) {
                return 0;
            }
            if (blob.Properties.Length == 0) {
                await blob.FetchAttributesAsync().ConfigureAwait(false);
            }
            return blob.Properties.Length;
        }

        public async Task SetLastModifiedAsync(String relative, DateTime dt) {
        }
        public async Task SetLastAccessAsync(String relative, DateTime dt) {
        }

        public void ParallelForFilesIn(String relative, Action<String> execFunc, Action<String, Exception> handler = null) {
            FixRelative(ref relative);
            try {
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                Parallel.ForEach(list, (a) => {
                    try {
                        execFunc(a);
                    } catch (Exception x) {
                        handler?.Invoke(a, x);
                    };
                });
            } catch (StorageException) { } catch (Exception) { }

        }
        public void ForFilesIn(String relative, Action<String> execFunc, Action<String, Exception> handler = null) {
            try {
                foreach (var a in GetFilesIn(relative)) {
                    try {
                        execFunc(a);
                    } catch (Exception x) {
                        handler?.Invoke(a, x);
                    };
                }
            } catch (StorageException sex) {
                Console.WriteLine(sex); // nice
            } catch (Exception ex) {
                Console.WriteLine(ex);
            }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            FixRelative(ref relative);
            var blobs = ListBlobs(relative);
            var list = blobs.Select(
                (a) => a.Name.Substring(BaseDirectory.Length))
                //.Where((a) => a.Count((b) => b == '/') == 0)
                .ToList();
            return list;
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            FixRelative(ref relative);
            var blobs = ListBlobs(relative);
            var list = blobs.Select(
                (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                .Where((a) => a.Count((b) => b == '/') == 0)
                .GroupBy((a) => a)
                .First();
            Parallel.ForEach(list, (a) => {
                execFunc(a);
            });
        }
        public void ForDirectoriesIn(String relative, Action<String> execFunc) {
            foreach (var a in GetDirectoriesIn(relative)) {
                execFunc(a);
            }
        }

        public List<CloudBlockBlob> ListBlobs(string relative) {
            BlobContinuationToken bucet = new BlobContinuationToken();
            BlobResultSegment result;
            if(relative.StartsWith("/")) {
                relative = relative.Substring(1);
            }
            List<CloudBlockBlob> retv = new List<CloudBlockBlob>();
            int numResults = 0;
            do {
                var reference = BlobContainer.GetDirectoryReference(relative);
                result = reference.ListBlobsSegmentedAsync(false, BlobListingDetails.Copy, 500, bucet, null, null).GetAwaiter().GetResult();
                bucet = result.ContinuationToken;
                var blobResults = result.Results.ToList();
                retv.AddRange(blobResults.Select(a => new CloudBlockBlob(a.Uri)));
                numResults = blobResults.Count;
            } while (bucet != null);

            return retv.Where(x=> x?.Name != null).ToList();
        }

        public IEnumerable<string> GetDirectoriesIn(String relative) {
            FixRelative(ref relative);
            BlobContinuationToken bucet = new BlobContinuationToken();
            BlobResultSegment result;
            var directory = BlobContainer.GetDirectoryReference(relative);
            int numResults = 0;
            do {
                result = directory.ListBlobsSegmentedAsync(bucet).Result;
                bucet = result.ContinuationToken;
                var folders = result.Results.Where(x=> x as CloudBlobDirectory != null);
                foreach(var folder in result.Results) {
                    var retv = folder.Uri.LocalPath.Replace($"/{this.ContainerName.ToLower()}", "").Replace(this.BaseDirectory, "").Substring(1);
                    if(retv.EndsWith("/")) {
                        retv = retv.Substring(0, retv.Length - 1);
                        yield return retv;
                    }
                }
            } while (numResults > 0);
            yield break;
        }

        public async Task<bool> Read(String relative, Func<Stream, Task> func) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            if (!blob.ExistsAsync().Result)
                return false;

            using (var stream = blob.OpenReadAsync().Result) {
                await func(stream);
            }

            return true;
        }
        public String ReadAllText(String relative) {
            return ReadAllTextAsync(relative).GetAwaiter().GetResult();
        }

        public void WriteAllText(String relative, String content) {
            WriteAllTextAsync(relative,content).GetAwaiter().GetResult();
        }

        public byte[] ReadAllBytes(String relative) {
            return ReadAllBytesAsync(relative).GetAwaiter().GetResult();
        }

        public void WriteAllBytes(String relative, byte[] content) {
            WriteAllBytesAsync(relative, content).GetAwaiter().GetResult();
        }

        public async Task<bool> DeleteAsync(String relative) {
            FixRelative(ref relative);
            if (!await ExistsAsync(relative).ConfigureAwait(false))
                return true;
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            await blob.DeleteAsync().ConfigureAwait(false);
            return true;
        }

        public async Task<bool> IsDirectoryAsync(string relative) {
            FixRelative(ref relative);
            return false;
        }
        public async Task<bool> IsFileAsync(string relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return await blob.ExistsAsync().ConfigureAwait(false);
        }

        public async Task<bool> ExistsAsync(String relative) {
            return await IsFileAsync(relative).ConfigureAwait(false);
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            FixRelative(ref relative);
            var blob = BlobContainer.GetAppendBlobReference(relative);
            if(!blob.ExistsAsync().Result) {
                blob.CreateOrReplaceAsync().Wait();
            }

            blob.AppendTextAsync(String.Join("\n", content)).Wait();
        }

        public async Task AppendAllLinesAsync(String relative, IEnumerable<string> content, Action OnComplete = null) {
            FixRelative(ref relative);
            var blob = BlobContainer.GetAppendBlobReference(relative);

            await blob.AppendTextAsync(String.Join("\n", content));
        }

        public async Task<Stream> OpenAsync(String relative, FileMode fileMode, FileAccess fileAccess) {
            FixRelative(ref relative);
            var blob = BlobContainer.GetAppendBlobReference(relative);
            if(
                fileMode == FileMode.Append
                || fileMode == FileMode.Create
                || fileMode == FileMode.CreateNew) {
                return await blob.OpenWriteAsync(fileMode == FileMode.CreateNew).ConfigureAwait(false);
            } else {
                return await blob.OpenReadAsync().ConfigureAwait(false);
            }
        }

        public void Hide(string relative) {
            // Blob accessor doesn't do that
        }

        public void Show(string relative) {
            // Blob accessor doesn't do that
        }

        public async Task<string> ReadAllTextAsync(string relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return await blob.DownloadTextAsync();
        }

        public async Task<byte[]> ReadAllBytesAsync(string relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            byte[] bytes = new byte[4096];
            using (MemoryStream ms = new MemoryStream()) {
                using (var stream = await blob.OpenReadAsync()) {
                    int bytesRead = 0;
                    do {
                        bytesRead = await stream.ReadAsync(bytes, 0, bytes.Length);
                        ms.Write(bytes, 0, bytesRead);
                    } while (bytesRead > 0);

                    return ms.ToArray();
                }
            }
        }

        public async Task WriteAllTextAsync(string relative, string content) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            await blob.UploadTextAsync(content).ConfigureAwait(false);
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            await blob.SetPropertiesAsync().ConfigureAwait(false);
        }

        public async Task WriteAllBytesAsync(string relative, byte[] content) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            await blob.UploadFromByteArrayAsync(content, 0, content.Length).ConfigureAwait(false);
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            await blob.SetPropertiesAsync().ConfigureAwait(false);
        }
    }
}
