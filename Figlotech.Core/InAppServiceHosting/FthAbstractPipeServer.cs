using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.Extensions;

namespace Figlotech.Core.InAppServiceHosting
{
    public abstract class FthAbstractPipeServer
    {

        NamedPipeServerStream server;
        Thread serverThread;
        public string pipeName { get; private set; }
        CancellationToken cancellationToken;

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
