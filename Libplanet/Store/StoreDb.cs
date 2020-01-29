// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;

namespace Libplanet.Store
{
    public class StoreDb : IDisposable
    {
        private readonly string dataFolder;

        public StoreDb(string folder)
        {
            dataFolder = folder;
            InitAndRecover();
        }

        public FasterKV<
            Types.StoreKey,
            Types.StoreValue,
            Types.StoreInput,
            Types.StoreOutput,
            Types.StoreContext,
            Types.StoreFunctions> Db { get; set; }

        public IDevice Log { get; set; }

        public IDevice ObjLog { get; set; }

        public bool InitAndRecover()
        {
            var logSize = 1L << 20;
            Log = Devices.CreateLogDevice(
                Path.Combine(dataFolder, "data", "Store-hlog.log"),
                preallocateFile: false);
            ObjLog = Devices.CreateLogDevice(
                Path.Combine(dataFolder, "data", "Store-hlog-obj.log"),
                preallocateFile: false);

            this.Db = new FasterKV<
                Types.StoreKey,
                Types.StoreValue,
                Types.StoreInput,
                Types.StoreOutput,
                Types.StoreContext,
                Types.StoreFunctions>(
                    logSize,
                    new Types.StoreFunctions(),
                    new LogSettings
                    {
                        LogDevice = this.Log,
                        ObjectLogDevice = this.ObjLog,
                        MutableFraction = 0.3,
                        PageSizeBits = 15,
                        MemorySizeBits = 20,
                    },
                    new CheckpointSettings
                    {
                        CheckpointDir = $"{dataFolder}/data/checkpoints",
                    },
                    new SerializerSettings<Types.StoreKey, Types.StoreValue>
                    {
                        keySerializer = () => new Types.StoreKeySerializer(),
                        valueSerializer = () => new Types.StoreValueSerializer(),
                    }
                );

            if (Directory.Exists($"{dataFolder}/data/checkpoints"))
            {
                Console.WriteLine("call recover db");
                Db.Recover();
                return false;
            }

            return true;
        }

        public Guid Checkpoint()
        {
            Db.TakeFullCheckpoint(out Guid token);
            Db.CompleteCheckpointAsync().GetAwaiter().GetResult();
            return token;
        }

        public void Dispose()
        {
            Db?.Dispose();
            Log?.Close();
            ObjLog?.Close();
        }
    }
}
