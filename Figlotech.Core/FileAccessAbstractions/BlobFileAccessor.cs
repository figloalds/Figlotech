using Figlotech.Core.Interfaces;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
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

    public class BlobFileAccessor : IFileSystem {

        private static int gid = 0;
        private static int myid = ++gid;

        String AccountName { get; set; }
        String AccountKey { get; set; }
        String ContainerName { get; set; }
        CloudBlobContainer BlobContainer;

        public void InitBlobFileAccessor(string host, string accountName, string containerName, string accountKey) {

            AccountName = accountName;
            AccountKey = containerName;
            ContainerName = containerName;

            string connectionString =
                (host != null ?
                    "UseDevelopmentStorage=true;" +
                    $"BlobEndpoint=http://{host}/{accountName};" 
                    :
                    (accountName != null ? $"AccountName={accountName};" : "") +
                    "DefaultEndpointsProtocol=https;"
                ) +
                (accountKey != null ? $"AccountKey={accountKey};" : "");

            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            BlobContainer = cloudBlobClient.GetContainerReference(containerName);

            BlobContainer.CreateIfNotExistsAsync().Wait();

        }

        public BlobFileAccessor(string host, string storageAccountName, string containerName, string storageAccountKey) {
            InitBlobFileAccessor(host, storageAccountName, containerName, storageAccountKey);
        }
        public BlobFileAccessor(string storageAccountName, string storageAccountKey, string containerName) {
            InitBlobFileAccessor(null, storageAccountName, containerName, storageAccountKey);
        }

        private void AbsMkDirs(string dir) {

            // NO WORK NEEDED HERE APPARENTLY
            // YAYPERS

        }

        public void MkDirs(string dir) {
            AbsMkDirs(dir);
        }

        public void Write(String relative, Action<Stream> func) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            using (var stream = blob.OpenWriteAsync().Result) {
                func(stream);
            }
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        private void RenDir(string relative, string newName) {
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
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            var newBlobRelative = relative.Substring(0, relative.LastIndexOf("/")) + newName;
            CloudBlockBlob newBlob = BlobContainer.GetBlockBlobReference(newBlobRelative);

            newBlob.StartCopyAsync(blob).Wait();

        }

        public DateTime? GetLastModified(string relative) {
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
            return GetLastModified(relative);
        }

        public long GetSize(string relative) {
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
            try {
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                //Fi.Tech.WriteLine($"List Blobs {relative} {blobs.Count((a) => true)} blobs");
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
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                //Fi.Tech.WriteLine($"List Blobs {relative} {blobs.Count((a) => true)} blobs");
                foreach (var a in list) {
                    try {
                        execFunc(a);
                    } catch (Exception x) {
                        handler?.Invoke(a, x);
                    };
                }
            } catch (StorageException) { } catch (Exception) { }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            try {
                var blobs = ListBlobs(relative);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                return list;
            } catch (StorageException) { } catch (Exception) { }
            return new string[0];
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
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
                result = BlobContainer.GetDirectoryReference(relative).ListBlobsSegmentedAsync(bucet).Result;
                var blobResults = result.Results.ToList();
                retv.AddRange(blobResults.Select(a => new CloudBlockBlob(a.Uri)));
                numResults = blobResults.Count;
            } while (numResults > 0);

            return retv;
        }

        public IEnumerable<string> GetDirectoriesIn(String relative) {
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
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            if (!blob.ExistsAsync().Result)
                return false;

            using (var stream = blob.OpenReadAsync().Result) {
                func(stream);
            }

            return true;
        }
        public String ReadAllText(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.DownloadTextAsync().Result;
        }

        public void WriteAllText(String relative, String content) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            blob.UploadTextAsync(content).Wait();
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        public byte[] ReadAllBytes(String relative) {
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
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            blob.UploadFromByteArrayAsync(content, 0, content.Length).Wait();
            blob.Properties.ContentType = Fi.Tech.GetMimeType(relative);
            blob.SetPropertiesAsync().Wait();
        }

        public bool Delete(String relative) {
            if (!Exists(relative))
                return true;
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            blob.DeleteAsync().Wait();
            return true;
        }

        public bool IsDirectory(string relative) {
            return false;
        }
        public bool IsFile(string relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.ExistsAsync().Result;
        }

        public bool Exists(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.ExistsAsync().Result;
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            var blob = BlobContainer.GetAppendBlobReference(relative);

            blob.AppendTextAsync(String.Join("\n", content)).Wait();
        }

        public async Task AppendAllLinesAsync(String relative, IEnumerable<string> content, Action OnComplete = null) {
            var blob = BlobContainer.GetAppendBlobReference(relative);

            await blob.AppendTextAsync(String.Join("\n", content));
        }

        public Stream Open(String relative, FileMode fileMode, FileAccess fileAccess) {
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
    }
}
