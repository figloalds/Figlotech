using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Figlotech.BDados.FileAcessAbstractions {
    public class MixedFileAccessor : IFileAccessor {

        List<FileAccessor> accessors = new List<FileAccessor>();
        public MixedFileAccessor(params FileAccessor[] inputAccessors) {
            accessors.AddRange(inputAccessors);
        }


        public void AppendAllLines(string relative, IEnumerable<string> content) {
            Parallel.ForEach(accessors, (a) => {
                a.AppendAllLines(relative, content);
            });
        }

        public void AppendAllLinesAsync(string relative, IEnumerable<string> content, Action OnComplete = null) {
            Parallel.ForEach(accessors, (a) => {
                a.AppendAllLines(relative, content);
            });
        }

        public bool Delete(string relative) {
            var retv = true;
            Parallel.ForEach(accessors, (a) => {
                bool p = a.Delete(relative);
                retv &= p;
            });

            return retv;
        }

        public bool Exists(string relative) {
            var retv = false;
            Parallel.ForEach(accessors, (a) => {
                if (retv) return;
                bool p = a.Exists(relative);
                retv |= p;
            });

            return retv;
        }

        public void ForDirectoriesIn(string relative, Action<string> execFunc) {
            Parallel.ForEach(accessors, (a) => {
                a.ForDirectoriesIn(relative, execFunc);
            });
        }

        public void ForFilesIn(string relative, Action<string> execFunc) {
            Parallel.ForEach(accessors, (a) => {
                a.ForFilesIn(relative, execFunc);
            });
        }

        public IEnumerable<string> GetDirectoriesIn(string relative) {
            Queue<String> li = new Queue<String>();
            Parallel.ForEach(accessors, (a) => {
                var dirs = a.GetDirectoriesIn(relative);
                foreach(var d in dirs) {
                    li.Enqueue(d);
                }
            });
            var retv = new List<String>();
            while(li.Count>0) {
                var str = li.Dequeue();
                if(!retv.Contains(str)) {
                    retv.Add(str);
                }
            }

            return retv;
        }

        public IEnumerable<string> GetFilesIn(string relative) {
            Queue<String> li = new Queue<String>();
            Parallel.ForEach(accessors, (a) => {
                var dirs = a.GetFilesIn(relative);
                foreach (var d in dirs) {
                    li.Enqueue(d);
                }
            });
            var retv = new List<String>();
            while (li.Count > 0) {
                var str = li.Dequeue();
                if (!retv.Contains(str)) {
                    retv.Add(str);
                }
            }

            return retv;
        }

        public DateTime? GetLastFileWrite(string relative) {
            var fws = new List<DateTime?>();
            Parallel.ForEach(accessors, (a) => {
                fws.Add(a.GetLastFileWrite(relative));
            });

            return fws.Where((a) => a != null).OrderByDescending((a) => a.Value.Ticks).FirstOrDefault();
        }

        public long GetSize(string relative) {
            var sizes = new List<long>();
            Parallel.ForEach(accessors, (a) => {
                sizes.Add(a.GetSize(relative));
            });
            sizes = sizes.Where((s) => s > 0).ToList();
            return sizes.Sum() / sizes.Count;
        }

        public bool IsDirectory(string relative) {
            return false;
        }

        public bool IsFile(string relative) {
            return false;
        }

        public void MkDirs(string relative) {
            Parallel.ForEach(accessors, (a) => {
                a.MkDirs(relative);
            });
        }

        public Stream Open(string relative, FileMode fileMode) {
            throw new NotImplementedException();
        }

        public bool Read(string relative, Action<Stream> func) {
            foreach(var a in accessors) {
                if(a.Exists(relative)) {
                    try {
                        a.Read(relative, func);
                        return true;
                    } catch(Exception) { }
                }
            }
            return false;
        }

        public byte[] ReadAllBytes(string relative) {
            foreach (var a in accessors) {
                if (a.Exists(relative)) {
                    try {
                        return a.ReadAllBytes(relative);
                    }
                    catch (Exception) { }
                }
            }
            return new byte[0];
        }

        public string ReadAllText(string relative) {
            foreach (var a in accessors) {
                if (a.Exists(relative)) {
                    try {
                        return a.ReadAllText(relative);
                    }
                    catch (Exception) { }
                }
            }
            return null;
        }

        public void Rename(string relative, string newName) {
            Parallel.ForEach(accessors, (a) => {
                a.Rename(relative, newName);
            });
        }

        public void SetLastModified(string relative, DateTime dt) {
            Parallel.ForEach(accessors, (a) => {
                a.SetLastModified(relative, dt);
            });
        }

        public void Write(string relative, Action<Stream> func) {
            Parallel.ForEach(accessors, (a) => {
                a.Write(relative, func);
            });
        }

        public void WriteAllBytes(string relative, byte[] content) {
            Parallel.ForEach(accessors, (a) => {
                a.WriteAllBytes(relative, content);
            });
        }

        public void WriteAllText(string relative, string content) {
            Parallel.ForEach(accessors, (a) => {
                a.WriteAllText(relative, content);
            });
        }
    }
}
