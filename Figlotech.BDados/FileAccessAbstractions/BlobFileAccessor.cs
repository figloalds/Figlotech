using Figlotech.BDados.Interfaces;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.FileAcessAbstractions {
    public class BlobMetaData {
        public string Relative;
        public long Length;
        public DateTime LastWrite;
    }

    public class BlobFileAccessor : IFileAccessor {

        private static int gid = 0;
        private static int myid = ++gid;

        String AccountName { get; set; }
        String AccountKey { get; set; }
        String ContainerName { get; set; }
        CloudBlobContainer BlobContainer;

        public BlobFileAccessor(string storageAccountName, string storageAccountKey, string containerName) {
            AccountName = storageAccountName;
            AccountKey = storageAccountKey;
            ContainerName = containerName;

            string connectionString = string.Format(
                @"DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                storageAccountName, storageAccountKey);
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);

            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            BlobContainer = cloudBlobClient.GetContainerReference(containerName);

            BlobContainer.CreateIfNotExists();
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
            using (var stream = blob.OpenWrite()) {
                func(stream);
            }
            blob.Properties.ContentType = FTH.GetMimeType(relative);
            blob.SetProperties(); 
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

            newBlob.StartCopy(blob);

        }

        public DateTime? GetLastFileWrite(string relative)
        {
            CloudBlob blob = BlobContainer.GetBlobReference(relative);
            if (!blob.Exists())
            {
                return DateTime.MinValue;
            }
            if (!blob.Properties.LastModified.HasValue || blob.Properties.LastModified == DateTime.MinValue) {
                blob.FetchAttributes();
            }
            var dt = blob.Properties.LastModified ?? DateTime.MinValue;
            return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc);
        }

        public long GetSize(string relative)
        {
            CloudBlob blob = BlobContainer.GetBlobReference(relative);
            if (!blob.Exists())
            {
                return 0;
            }
            if (blob.Properties.Length == 0) {
                blob.FetchAttributes();
            }
            return blob.Properties.Length;
        }

        public void SetLastModified(String relative, DateTime dt)
        {
        }

        public void ParallelForFilesIn(String relative, Action<String> execFunc) {
            try {
                if (relative.StartsWith("/"))
                    relative = relative.Substring(1);
                var diref = BlobContainer.GetDirectoryReference(relative);
                var blobs = diref.ListBlobs(true, BlobListingDetails.All);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                //FTH.WriteLine($"List Blobs {relative} {blobs.Count((a) => true)} blobs");
                Parallel.ForEach(list, (a) => { 
                    try {
                        execFunc(a);
                    }
                    catch (Exception x) {
                    };
                });
            } catch (StorageException) { } 
            catch (Exception) { }
            
        }
        public void ForFilesIn(String relative, Action<String> execFunc) {
            try {
                if (relative.StartsWith("/"))
                    relative = relative.Substring(1);
                var diref = BlobContainer.GetDirectoryReference(relative);
                var blobs = diref.ListBlobs(true, BlobListingDetails.All);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                //FTH.WriteLine($"List Blobs {relative} {blobs.Count((a) => true)} blobs");
                foreach (var a in list) {
                    try {
                        execFunc(a);
                    }
                    catch (Exception x) {
                    };
                }
            }
            catch (StorageException) { }
            catch (Exception) { }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            try {
                if (relative.StartsWith("/"))
                    relative = relative.Substring(1);
                var diref = BlobContainer.GetDirectoryReference(relative);
                var blobs = diref.ListBlobs(true, BlobListingDetails.All);
                var list = blobs.Select(
                    (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                    .Where((a) => a.Count((b) => b == '/') == 0)
                    .GroupBy((a) => a)
                    .First();
                return list;
            }
            catch (StorageException) { }
            catch (Exception) { }
            return new string[0];
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            var blobs = BlobContainer.GetDirectoryReference(relative).ListBlobs(true);
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
            var blobs = BlobContainer.GetDirectoryReference(relative).ListBlobs(true);
            var list = blobs.Select(
                (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length + relative.Length + 2))
                .Where((a) => a.Count((b) => b == '/') == 0)
                .GroupBy((a) => a)
                .First();
            foreach (var a in list) {
                execFunc(a);
            }
        }
        public IEnumerable<string> GetDirectoriesIn(String relative) {
            var blobs = BlobContainer.GetDirectoryReference(relative).ListBlobs(true);
            var list = blobs.Select(
                (a) => a.Uri.PathAndQuery.Substring(0, ContainerName.Length+relative.Length+2))
                .Where((a)=>a.Count((b)=>b=='/') == 0)
                .GroupBy((a) => a)
                .First();
            return list;
        }

        public bool Read(String relative, Action<Stream> func) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            if (!blob.Exists())
                return false;

            using (var stream = blob.OpenRead()) {
                func(stream);
            }

            return true;
        }
        public String ReadAllText(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.DownloadText(Encoding.UTF8);
        }

        public void WriteAllText(String relative, String content) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            blob.UploadText(content, Encoding.UTF8);
            blob.Properties.ContentType = FTH.GetMimeType(relative);
            blob.SetProperties();
        }

        public byte[] ReadAllBytes(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            byte[] bytes = new byte[4096];
            lock (relative) {
                using (MemoryStream ms = new MemoryStream()) {
                    using (var stream = blob.OpenRead()) {
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

            blob.UploadFromByteArray(content, 0, content.Length);
            blob.Properties.ContentType = FTH.GetMimeType(relative);
            blob.SetProperties();
        }

        public bool Delete(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);

            FTH.GlobalQueuer.Enqueue(() => {
                blob.Delete();
            });
            
            return true;
        }

        public bool IsDirectory(string relative) {
            return false;
        }
        public bool IsFile(string relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.Exists();
        }

        public bool Exists(String relative) {
            CloudBlockBlob blob = BlobContainer.GetBlockBlobReference(relative);
            return blob.Exists();
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            var blob = BlobContainer.GetAppendBlobReference(relative);

            FTH.GlobalQueuer.Enqueue(() => {
                blob.AppendText(String.Join("\n", content), Encoding.UTF8);
            });


        }

        public void AppendAllLinesAsync(String relative, IEnumerable<string> content, Action OnComplete = null) {
            var blob = BlobContainer.GetAppendBlobReference(relative);

            FTH.GlobalQueuer.Enqueue(() => {
                blob.AppendText(String.Join("\n", content), Encoding.UTF8);
            });


        }
    }
}
