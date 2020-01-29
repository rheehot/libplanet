// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

namespace Libplanet.Store
{
    public class Types
    {
        public static byte[] Hash256(byte[] byteContents)
        {
            using (var hash = new System.Security.Cryptography.SHA256CryptoServiceProvider())
            {
                return hash.ComputeHash(byteContents);
            }
        }

        public class StoreKey : IFasterEqualityComparer<StoreKey>
        {
            public byte[] Key { get; set; }

            public string TableType { get; set; }

            public virtual long GetHashCode64(ref StoreKey k)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(k.TableType);
                byte[] b = bytes.Concat(k.Key).ToArray();

                var hash256 = Hash256(b);

                return hash256.GetHashCode();
            }

            public virtual bool Equals(ref StoreKey k1, ref StoreKey k2)
            {
                return k1.Key.SequenceEqual(k2.Key) && k1.TableType == k2.TableType;
            }
        }

        public class StoreKeySerializer : BinaryObjectSerializer<StoreKey>
        {
            public override void Deserialize(ref StoreKey obj)
            {
                var bytesr = new byte[4];
                reader.Read(bytesr, 0, 4);
                var sizet = BitConverter.ToInt32(bytesr, 0);
                var bytes = new byte[sizet];
                reader.Read(bytes, 0, sizet);
                obj.TableType = System.Text.Encoding.UTF8.GetString(bytes);

                bytesr = new byte[4];
                reader.Read(bytesr, 0, 4);
                var size = BitConverter.ToInt32(bytesr, 0);
                obj.Key = new byte[size];
                reader.Read(obj.Key, 0, size);
            }

            public override void Serialize(ref StoreKey obj)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(obj.TableType);
                var len = BitConverter.GetBytes(bytes.Length);
                writer.Write(len);
                writer.Write(bytes);

                len = BitConverter.GetBytes(obj.Key.Length);
                writer.Write(len);
                writer.Write(obj.Key);
            }
        }

        public class StoreValue
        {
            public byte[] Value { get; set; }
        }

        public class StoreValueSerializer : BinaryObjectSerializer<StoreValue>
        {
            public override void Deserialize(ref StoreValue obj)
            {
                var bytesr = new byte[4];
                reader.Read(bytesr, 0, 4);
                int size = BitConverter.ToInt32(bytesr, 0);
                obj.Value = reader.ReadBytes(size);
            }

            public override void Serialize(ref StoreValue obj)
            {
                var len = BitConverter.GetBytes(obj.Value.Length);
                writer.Write(len);
                writer.Write(obj.Value);
            }
        }

        public class StoreInput
        {
            public byte[] Value { get; set; }
        }

        public class StoreOutput
        {
            public StoreValue Value { get; set; }
        }

        public class StoreContext
        {
            private Status _status;
            private StoreOutput _output;

            internal void Populate(ref Status status, ref StoreOutput output)
            {
                this._status = status;
                this._output = output;
            }

            internal void FinalizeRead(ref Status status, ref StoreOutput output)
            {
                status = this._status;
                output = this._output;
            }
        }

        public class StoreFunctions
            : IFunctions<StoreKey, StoreValue, StoreInput, StoreOutput, StoreContext>
        {
            public void RMWCompletionCallback(
                ref StoreKey key, ref StoreInput input, StoreContext ctx, Status status)
            {
                // Method intentionally left empty.
            }

            public void ReadCompletionCallback(
                ref StoreKey key,
                ref StoreInput input,
                ref StoreOutput output,
                StoreContext ctx,
                Status status)
            {
                ctx.Populate(ref status, ref output);
            }

            public void UpsertCompletionCallback(
                ref StoreKey key, ref StoreValue value, StoreContext ctx)
            {
                // Method intentionally left empty.
            }

            public void DeleteCompletionCallback(ref StoreKey key, StoreContext ctx)
            {
                // Method intentionally left empty.
            }

            public void CopyUpdater(
                ref StoreKey key,
                ref StoreInput input,
                ref StoreValue oldValue,
                ref StoreValue newValue)
            {
                // Method intentionally left empty.
            }

            public void InitialUpdater(ref StoreKey key, ref StoreInput input, ref StoreValue value)
            {
                // Method intentionally left empty.
            }

            public bool InPlaceUpdater(ref StoreKey key, ref StoreInput input, ref StoreValue value)
            {
                if (value.Value.Length < input.Value.Length)
                {
                    return false;
                }

                value.Value = input.Value;
                return true;
            }

            public void SingleReader(
                ref StoreKey key, ref StoreInput input, ref StoreValue value, ref StoreOutput dst)
            {
                dst.Value = value;
            }

            public void ConcurrentReader(
                ref StoreKey key, ref StoreInput input, ref StoreValue value, ref StoreOutput dst)
            {
                dst.Value = value;
            }

            public bool ConcurrentWriter(ref StoreKey key, ref StoreValue src, ref StoreValue dst)
            {
                if (src == null)
                {
                    return false;
                }

                if (dst.Value.Length != src.Value.Length)
                {
                    return false;
                }

                dst = src;
                return true;
            }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
                // Method intentionally left empty.
            }

            public void SingleWriter(ref StoreKey key, ref StoreValue src, ref StoreValue dst)
            {
                dst = src;
            }
        }
    }
}
