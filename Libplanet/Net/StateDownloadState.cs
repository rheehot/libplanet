namespace Libplanet.Net
{
    /// <summary>
    /// Indicates a progress of downloading state references.
    /// </summary>
    public class StateDownloadState : PreloadState
    {
        /// <summary>
        /// Total number of state references to receive in the current batch.
        /// </summary>
        public int TotalIterationCount { get; internal set; }

        /// <summary>
        /// The number of received state references until now.
        /// </summary>
        public int ReceivedIterationCount { get; internal set; }

        /// <inheritdoc />
        public override int CurrentPhase => 2;
    }
}