﻿using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Extensions
{
    public static class FiStreamExtensions {
        private static Type[] AllowedTypes = new Type[] {
            typeof(Int16),
            typeof(Int32),
            typeof(Int64),
            typeof(UInt16),
            typeof(UInt32),
            typeof(UInt64),
            typeof(Single),
            typeof(Double),
            typeof(Char),
            typeof(Boolean),
        };
        private static Dictionary<Type, int> AllowedSizes = new Dictionary<Type, int> {
            { typeof(Int16), sizeof(Int16) },
            { typeof(Int32), sizeof(Int32) },
            { typeof(Int64), sizeof(Int64) },
            { typeof(UInt16), sizeof(UInt16) },
            { typeof(UInt32), sizeof(UInt32) },
            { typeof(UInt64), sizeof(UInt64) },
            { typeof(Single), sizeof(Single) },
            { typeof(Double), sizeof(Double) },
            { typeof(Char), sizeof(Char) },
            { typeof(Boolean), sizeof(Boolean) },
        };

        /// <summary>
        /// This copies one stream to the other comparing bytes on each one
        /// Reading the destination stream first to check if its necessary to write
        /// and so saving unnecessary write (may be cool sometimes, for stuff like for file copy)
        /// </summary>
        /// <param name="origin"></param>
        /// <param name="destination"></param>
        public static async Task EconomicCopyToAsync(this Stream origin, Stream destination) {
            origin.Seek(0, SeekOrigin.Begin);
            destination.Seek(0, SeekOrigin.Begin);
            var bufferSize = 256 * 1024;
            if(origin.Length < bufferSize * 2) {
                await origin.CopyToAsync(destination).ConfigureAwait(false);
            }
            var originBuffer = new byte[bufferSize];
            var destinationBuffer = new byte[bufferSize];
            int bytesRead = 0;
            while ((bytesRead = await origin.ReadAsync(originBuffer, 0, originBuffer.Length).ConfigureAwait(false)) > 0) {
                await destination.ReadAsync(destinationBuffer, 0, bytesRead).ConfigureAwait(false);
                if (!originBuffer.SequenceEqual(destinationBuffer)) {
                    destination.Seek(origin.Position - bytesRead, SeekOrigin.Begin);
                    await destination.WriteAsync(originBuffer, 0, bytesRead).ConfigureAwait(false);
                }
            }
        }

        public static T Read<T>(this Stream me) {
            Action err = () => {
                var supportedTypes = String.Join(", ", AllowedTypes.Select(t => t.Name));
                throw new ArgumentException($"Generic type {typeof(T).Name} for FiStreamExtensions.Read<T> is invalid, supported types are: { supportedTypes }");
            };

            if(!AllowedSizes.ContainsKey(typeof(T))) {
                err();
            }
            var len = AllowedSizes[typeof(T)];
            var buff = new byte[len];
            me.Read(buff, 0, len);
            
            var methods = typeof(BitConverter).GetMethods();
            var method = methods.FirstOrDefault(m => m.Name == $"To{typeof(T).Name}");
            if(method == null) {
                err();
            }
            T retv = (T)method.Invoke(me, new Object[] { buff, 0 });
            return retv;
        }

        public static void Write<T>(this Stream me, T val) {
            if(val is String || val is string) {
                Write(me, val as String, Fi.StandardEncoding);
                return;
            }
            Action err = () => {
                var supportedTypes = String.Join(", ", AllowedTypes.Select(t => t.Name));
                throw new ArgumentException($"Generic type {typeof(T).Name} for FiStreamExtensions.Read<T> is invalid, supported types are: { supportedTypes }");
            };

            if (!AllowedSizes.ContainsKey(typeof(T))) {
                err();
            }
            var len = AllowedSizes[typeof(T)];
            var buff = new byte[len];

            var methods = typeof(BitConverter).GetMethods();
            var method = methods.FirstOrDefault(m => m.Name == $"GetBytes" && m.GetParameters().FirstOrDefault()?.ParameterType == typeof(T));
            if (method == null) {
                err();
            }

            buff = (byte[]) method.Invoke(me, new Object[] { val });
            me.Write(buff, 0, len);
        }

        public static void Write(this Stream me, String text, Encoding encoding = null) {
            if (encoding == null)
                encoding = Fi.StandardEncoding;
            var buff = encoding.GetBytes(text);
            me.Write(buff, 0, buff.Length);
        }
        public static void WriteLine(this Stream me, String text, Encoding encoding = null) {
            me.Write(text+"\n", encoding);
        }
    }
}
