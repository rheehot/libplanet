using System.Security.Cryptography;

namespace Libplanet.Net
{
    /// <summary>
    /// Indicates a progress of downloading blocks.
    /// </summary>
    [Equals]
    public class BlockDownloadState : PreloadState
    {
        public BlockDownloadState(
            HashDigest<SHA256> receivedBlockHash,
            long totalBlockCount,
            long receivedBlockCount,
            BoundPeer sourcePeer)
        {
            ReceivedBlockHash = receivedBlockHash;
            TotalBlockCount = totalBlockCount;
            ReceivedBlockCount = receivedBlockCount;
            SourcePeer = sourcePeer;
        }

        /// <summary>
        /// The hash digest of the block just received.
        /// </summary>
        public HashDigest<SHA256> ReceivedBlockHash { get; internal set; }

        /// <summary>
        /// Total number of blocks to receive in the current batch.
        /// </summary>
        public long TotalBlockCount { get; internal set; }

        /// <summary>
        /// The number of currently received blocks.
        /// </summary>
        public long ReceivedBlockCount { get; internal set; }

        /// <inheritdoc />
        public override int CurrentPhase => 1;

        /// <summary>
        /// The peer which sent the block.
        /// </summary>
        public BoundPeer SourcePeer { get; internal set; }
    }
}
