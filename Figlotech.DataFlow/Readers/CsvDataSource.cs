using Figlotech.Core.Data;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.DataFlow.Interfaces;
using Figlotech.DataFlow.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.DataFlow.Readers
{
    public sealed class CsvDataSource : IDataTransform {

        String FilePath { get; set; }
        Stream FileStream { get; set; }
        StreamReader Reader { get; set; }
        IFileSystem FileSystem { get; set; }
        bool IsOpen { get; set; }
        SimpleCSVFormatter Formatter { get; set; }
        bool HasHeaderInFile { get; set; }
        int CurrentRow { get; set; } = 0;
        string[] Headers { get; set; } = new string[0];

        public CsvDataSource(IFileSystem fs, string path, char separator, bool hasHeaderLineInFile) {
            this.FileSystem = fs;
            this.FilePath = path;
            this.Formatter = new SimpleCSVFormatter(separator);
            this.HasHeaderInFile = hasHeaderLineInFile;
        }

        public object[] Current { get; private set; }

        public void Dispose() {
            this.FileStream.Dispose();
            this.Reader.Dispose();
            IsOpen = false;
        }

        ~CsvDataSource() {
            this.Dispose();
        }
        
        public async Task<string[]> GetHeaders() {
            if(HasHeaderInFile && CurrentRow == 0) {
                var line = await this.Reader.ReadLineAsync();
                CurrentRow++;
                if(line != null) {
                    return (Headers = this.Formatter.LineToData(line));
                }
            }
            return Headers;
        }

        public Task Initialize() {
            if(IsOpen) {
                Dispose();
            }
            this.FileStream = FileSystem.Open(FilePath, FileMode.Open, FileAccess.Read);
            this.Reader = new StreamReader(this.FileStream);
            IsOpen = true;
            CurrentRow = 0;
            return Task.CompletedTask;
        }

        public async Task<bool> Next() {
            if (HasHeaderInFile && CurrentRow == 0) {
                await GetHeaders();
            }
            var line = await this.Reader.ReadLineAsync();
            CurrentRow++;
            if (line != null) {
                var data = this.Formatter.LineToData(line);
                this.Current = data;
                return true;
            } else {
                return false;
            }
        }
        
        public IEnumerator<object[]> GetEnumerator() {
            return new DataTransformEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return new DataTransformEnumerator(this);
        }
    }
}
