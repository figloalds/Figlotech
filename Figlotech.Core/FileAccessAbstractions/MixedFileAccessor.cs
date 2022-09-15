//using Figlotech.Core.Interfaces;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using System.IO;

//namespace Figlotech.Core.FileAcessAbstractions {
//    public sealed class MixedFileAccessor : IFileSystem {

//        List<FileAccessor> accessors = new List<FileAccessor>();
//        public MixedFileAccessor(params FileAccessor[] inputAccessors) {
//            accessors.AddRange(inputAccessors);
//        }

//        public bool IsCaseSensitive => false;

//        public void AppendAllLines(string relative, IEnumerable<string> content) {
//            Parallel.ForEach(accessors, (a) => {
//                a.AppendAllLines(relative, content);
//            });
//        }

//        public void AppendAllLinesAsync(string relative, IEnumerable<string> content, Action OnComplete = null) {
//            Parallel.ForEach(accessors, (a) => {
//                a.AppendAllLines(relative, content);
//            });
//        }

//        public bool Delete(string relative) {
//            var retv = true;
//            Parallel.ForEach(accessors, (a) => {
//                bool p = a.Delete(relative);
//                retv &= p;
//            });

//            return retv;
//        }

//        public bool Exists(string relative) {
//            var retv = false;
//            Parallel.ForEach(accessors, (a) => {
//                if (retv) return;
//                bool p = a.Exists(relative);
//                retv |= p;
//            });

//            return retv;
//        }

//        public void ForDirectoriesIn(string relative, Action<string> execFunc) {
//            Parallel.ForEach(accessors, (a) => {
//                a.ForDirectoriesIn(relative, execFunc);
//            });
//        }

//        public void ForFilesIn(string relative, Action<string> execFunc, Action<string, Exception> handler = null) {
//            Parallel.ForEach(accessors, (a) => {
//                a.ForFilesIn(relative, execFunc, handler);
//            });
//        }

//        public IEnumerable<string> GetDirectoriesIn(string relative) {
//            Queue<String> li = new Queue<String>();
//            Parallel.ForEach(accessors, (a) => {
//                var dirs = a.GetDirectoriesIn(relative);
//                foreach(var d in dirs) {
//                    li.Enqueue(d);
//                }
//            });
//            var retv = new List<String>();
//            while(li.Count>0) {
//                var str = li.Dequeue();
//                if(!retv.Contains(str)) {
//                    retv.Add(str);
//                }
//            }

//            return retv;
//        }

//        public IEnumerable<string> GetFilesIn(string relative) {
//            Queue<String> li = new Queue<String>();
//            Parallel.ForEach(accessors, (a) => {
//                var dirs = a.GetFilesIn(relative);
//                foreach (var d in dirs) {
//                    li.Enqueue(d);
//                }
//            });
//            var retv = new List<String>();
//            while (li.Count > 0) {
//                var str = li.Dequeue();
//                if (!retv.Contains(str)) {
//                    retv.Add(str);
//                }
//            }

//            return retv;
//        }

//        public DateTime? GetLastModified(string relative) {
//            var fws = new List<DateTime?>();
//            Parallel.ForEach(accessors, (a) => {
//                fws.Add(a.GetLastModified(relative));
//            });

//            return fws.Where((a) => a != null).OrderByDescending((a) => a.Value.Ticks).FirstOrDefault();
//        }
//        public DateTime? GetLastAccess(string relative) {
//            var fws = new List<DateTime?>();
//            Parallel.ForEach(accessors, (a) => {
//                fws.Add(a.GetLastAccess(relative));
//            });

//            return fws.Where((a) => a != null).OrderByDescending((a) => a.Value.Ticks).FirstOrDefault();
//        }

//        public long GetSize(string relative) {
//            var sizes = new List<long>();
//            Parallel.ForEach(accessors, (a) => {
//                sizes.Add(a.GetSize(relative));
//            });
//            sizes = sizes.Where((s) => s > 0).ToList();
//            return sizes.Sum() / sizes.Count;
//        }

//        public bool IsDirectory(string relative) {
//            return false;
//        }

//        public bool IsFile(string relative) {
//            return false;
//        }

//        public void MkDirs(string relative) {
//            Parallel.ForEach(accessors, (a) => {
//                a.MkDirs(relative);
//            });
//        }

//        public Stream Open(string relative, FileMode fileMode, FileAccess fileAccess) {
//            throw new NotImplementedException();
//        }

//        public async Task<bool> Read(string relative, Func<Stream, Task> func) {
//            foreach(var a in accessors) {
//                if(a.Exists(relative)) {
//                    try {
//                        await a.Read(relative, func);
//                        return true;
//                    } catch(Exception) { }
//                }
//            }
//            return false;
//        }

//        public byte[] ReadAllBytes(string relative) {
//            foreach (var a in accessors) {
//                if (a.Exists(relative)) {
//                    try {
//                        return a.ReadAllBytes(relative);
//                    }
//                    catch (Exception) { }
//                }
//            }
//            return new byte[0];
//        }

//        public string ReadAllText(string relative) {
//            foreach (var a in accessors) {
//                if (a.Exists(relative)) {
//                    try {
//                        return a.ReadAllText(relative);
//                    }
//                    catch (Exception) { }
//                }
//            }
//            return null;
//        }

//        public void Rename(string relative, string newName) {
//            Parallel.ForEach(accessors, (a) => {
//                a.Rename(relative, newName);
//            });
//        }

//        public void SetLastModified(string relative, DateTime dt) {
//            Parallel.ForEach(accessors, (a) => {
//                a.SetLastModified(relative, dt);
//            });
//        }
//        public void SetLastAccess(string relative, DateTime dt) {
//            Parallel.ForEach(accessors, (a) => {
//                a.SetLastAccess(relative, dt);
//            });
//        }

//        public async Task Write(string relative, Action<Stream> func) {
//            Parallel.ForEach(accessors, (a) => {
//                a.Write(relative, func);
//            });
//        }

//        public void WriteAllBytes(string relative, byte[] content) {
//            Parallel.ForEach(accessors, (a) => {
//                a.WriteAllBytes(relative, content);
//            });
//        }

//        public void WriteAllText(string relative, string content) {
//            Parallel.ForEach(accessors, (a) => {
//                a.WriteAllText(relative, content);
//            });
//        }

//        public void Hide(string relative) {
//            Parallel.ForEach(accessors, (a) => {
//                a.Hide(relative);
//            });
//        }

//        public void Show(string relative) {
//            Parallel.ForEach(accessors, (a) => {
//                a.Show(relative);
//            });
//        }
//    }
//}
