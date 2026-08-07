using Figlotech.Extensions;
using System;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting {
    public abstract class FthAbstractPipeServer : IAsyncDisposable, IDisposable {

        public async ValueTask DisposeAsync() {
            Stop();
            if (serverThread != null) {
                serverThread.Join();
            }
            if (server != null) {
                await server.DisposeAsync();
            }
        }
        public void Dispose() {
            if (server != null) {
                server.Dispose();
            }
        }

        NamedPipeServerStream server;
        Thread serverThread;
        public string pipeName { get; private set; }
        readonly CancellationToken cancellationToken;

        public FthAbstractPipeServer(string pipeName) {
            this.pipeName = pipeName;
        }

        public abstract void Init(params object[] args);

        bool ServerStop = false;

        public void Start() {
            Init();
            serverThread = Fi.Tech.SafeCreateThread(() => {
                while (!ServerStop) {
                    using (server = new NamedPipeServerStream(pipeName)) {
                        server.WaitForConnection();
                        var init = server.ReadByte();
                        var len = server.Read<int>();
                        var msg = new byte[len];
                        server.Read(msg, 0, msg.Length);
                        var end = server.ReadByte();
                        var msgText = Fi.StandardEncoding.GetString(msg);

                        var respText = Process(msgText);

                        server.WriteByte(0x02);
                        var respBytes = Fi.StandardEncoding.GetBytes(respText);
                        server.Write<int>(respBytes.Length);
                        server.Write(respBytes, 0, respBytes.Length);
                        server.WriteByte(0x03);

                        server.WaitForPipeDrain();
                    }
                }
            });
            serverThread.SetApartmentState(ApartmentState.STA);
            serverThread.Start();
        }

        public void Stop() {
            ServerStop = true;
        }

        public abstract string Process(string incoming);
    }
}
