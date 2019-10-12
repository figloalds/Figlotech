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
    public class BlobMetaData {
        public string Relative;
        public long Length;
        public DateTime LastWrite;
    }

    public class BlobsCredentials {
        public string AccountName { get; set; }
        public string AccountKey { get; set; }

        public static BlobsCredentials Annonymous(String accountName) {
            return new BlobsCredentials { AccountName = accountName };
        }
    }

    public class BlobFileAccessor : IFileSystem {

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

            if (!IsReadOnly) {
                BlobContainer.CreateIfNotExistsAsync().Wait();
            }
        }

        string BaseDirectory { get; set; }

        public void SetBaseDirectory(string path) {
            BaseDirectory = path.Replace(Path.DirectorySeparatorChar, '/');
            if(!BaseDirectory.EndsWith("/")) {
                BaseDirectory += '/';
            }
        }

        public BlobFileAccessor(string host, BlobsCredentials credentials, string containerName) {
            InitBlobFileAccessor(host, credentials?.AccountName, containerName, credentials?.AccountKey);
        }
        public BlobFileAccessor(BlobsCredentials credentials, string containerName) {
            InitBlobFileAccessor(null, credentials?.AccountName, containerName, credentials?.AccountKey);
        }

        private void AbsMkDirs(string dir) {

            // NO WORK NEEDED HERE APPARENTLY
            // YAYPERS

        }

        public void MkDirs(string dir) {
            AbsMkDirs(dir);
        }

        string FixRelative(ref string relative) {
            if(relative.StartsWith("/")) {
                relative = relative.Substring(1);
            }
            relative = BaseDirectory + relative.Replace(Path.DirectorySeparatorChar, '/');
            return relative;
        }

        public void Write(String relative, Action<Stream> func) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            using (var stream = blob.OpenWriteAsync().Result) {
                func(stream);
            }
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        private void RenDir(string relative, string newName) {
            FixRelative(ref relative);
            foreach (var f in Directory.GetFiles(relative)) {
                lock (newName) {
                    lock (relative) {
                        var fname = f;
                        var fnew = f.Replace(relative, newName);
                        if (File.Exists(fname))
                            File.Delete(fname);
                        File.Move(fname, fnew);
                    }
                }
            }
            foreach (var f in Directory.GetDirectories(relative)) {
                lock (newName) {
                    lock (relative) {
                        RenDir(f, f.Replace(relative, newName));
                    }
                }
            }

            Directory.Delete(relative);
        }

        public void Rename(string relative, string newName) {
            FixRelative(ref relative);
            FixRelative(ref newName);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            CloudBlockBlob newBlob = BlobContainer.GetBlockBlobReference(newName);

            newBlob.StartCopyAsync(blob).Wait();
            blob.DeleteAsync().Wait();
        }

        public DateTime? GetLastModified(string relative) {
            FixRelative(ref relative);
            CloudBlob blob = BlobContainer.GetBlobReference(relative);
            if (!blob.ExistsAsync().Result) {
                return DateTime.MinValue;
            }
            if (!blob.Properties.LastModified.HasValue || blob.Properties.LastModified == DateTime.MinValue) {
                blob.FetchAttributesAsync().Wait();
            }
            var dt = blob.Properties.LastModified ?? DateTime.MinValue;
            return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc);
        }
        public DateTime? GetLastAccess(string relative) {
            FixRelative(ref relative);
            return GetLastModified(relative);
        }

        public long GetSize(string relative) {
            FixRelative(ref relative);
            CloudBlob blob = BlobContainer.GetBlobReference(relative);
            if (!blob.ExistsAsync().Result) {
                return 0;
            }
            if (blob.Properties.Length == 0) {
                blob.FetchAttributesAsync().Wait();
            }
            return blob.Properties.Length;
        }

        public void SetLastModified(String relative, DateTime dt) {
        }
        public void SetLastAccess(String relative, DateTime dt) {
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
            FixRelative(ref relative);
            try {
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Name.Replace(BaseDirectory, ""))
                    //.Where((a) => a.Count((b) => b == '/') == 0)
                    .ToList();
                foreach (var a in list) {
                    try {
                        execFunc(a);
                    } catch (Exception x) {
                        handler?.Invoke(a, x);
                    };
                }
            } catch (StorageException sex) {
                Console.WriteLine(sex); // lmao
            } catch (Exception ex) {
                Console.WriteLine(ex);
            }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            FixRelative(ref relative);
            try {
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Name.Replace(BaseDirectory, ""))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                return list;
            } catch (StorageException) { } catch (Exception) { }
            return new string[0];
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
            FixRelative(ref relative);
            var blobs = ListBlobs(relative);
            var list = blobs.Select(
                (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                .Where((a) => a.Count((b) => b == '/') == 0)
                .GroupBy((a) => a)
                .First();
            foreach (var a in list) {
                execFunc(a);
            }
        }

        public List<CloudBlockBlob> ListBlobs(string relative) {
            BlobContinuationToken bucet = new BlobContinuationToken();
            BlobResultSegment result;
            List<CloudBlockBlob> retv = new List<CloudBlockBlob>();
            int numResults = 0;
            do {
                result = BlobContainer.GetDirectoryReference(relative).ListBlobsSegmentedAsync(true, BlobListingDetails.Snapshots, 500, bucet, null, null).Result;
                bucet = result.ContinuationToken;
                var blobResults = result.Results.ToList();
                retv.AddRange(blobResults.Select(a => new CloudBlockBlob(a.Uri)));
                numResults = blobResults.Count;
            } while (numResults >= 500);

            return retv;
        }

        public IEnumerable<string> GetDirectoriesIn(String relative) {
            FixRelative(ref relative);
            BlobContinuationToken bct = new BlobContinuationToken();
            var blobs = ListBlobs(relative);
            var list = blobs.Select(
                (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                .Where((a) => a.Count((b) => b == '/') == 0)
                .GroupBy((a) => a)
                .First();
            return list;
        }

        public bool Read(String relative, Action<Stream> func) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            if (!blob.ExistsAsync().Result)
                return false;

            using (var stream = blob.OpenReadAsync().Result) {
                func(stream);
            }

            return true;
        }
        public String ReadAllText(String relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.DownloadTextAsync().Result;
        }

        public void WriteAllText(String relative, String content) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            blob.UploadTextAsync(content).Wait();
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        public byte[] ReadAllBytes(String relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            byte[] bytes = new byte[4096];
            lock (relative) {
                using (MemoryStream ms = new MemoryStream()) {
                    using (var stream = blob.OpenReadAsync().Result) {
                        int bytesRead = 0;
                        do {
                            bytesRead = stream.Read(bytes, 0, bytes.Length);
                            ms.Write(bytes, 0, bytesRead);
                        } while (bytesRead > 0);

                        return ms.ToArray();
                    }
                }
            }
        }

        public void WriteAllBytes(String relative, byte[] content) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            blob.UploadFromByteArrayAsync(content, 0, content.Length).Wait();
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        public bool Delete(String relative) {
            FixRelative(ref relative);
            if (!Exists(relative))
                return true;
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            blob.DeleteAsync().Wait();
            return true;
        }

        public bool IsDirectory(string relative) {
            FixRelative(ref relative);
            return false;
        }
        public bool IsFile(string relative) {
            FixRelative(ref relative);
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.ExistsAsync().Result;
        }

        public bool Exists(String relative) {
            return IsFile(relative);
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

        public Stream Open(String relative, FileMode fileMode, FileAccess fileAccess) {
            FixRelative(ref relative);
            var blob = BlobContainer.GetAppendBlobReference(relative);
            if(
                fileMode == FileMode.Append
                || fileMode == FileMode.Create
                || fileMode == FileMode.CreateNew) {
                return blob.OpenWriteAsync(fileMode == FileMode.CreateNew).Result;
            } else {
                return blob.OpenReadAsync().Result;
            }
        }

        public void Hide(string relative) {
            // Blob accessor doesn't do that
        }

        public void Show(string relative) {
            // Blob accessor doesn't do that
        }
    }
}
