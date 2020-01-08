using Secp256k1Net;

namespace Libplanet.Crypto
{
    public static class Secp256K1
    {
        private static readonly Secp256k1 _instance = new Secp256k1();

        public static Secp256k1 Instance => _instance;
    }
}
