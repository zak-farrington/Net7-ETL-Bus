using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Net7EtlBus.Service.Utilities
{
    public static class FileSystem
    {
        /// <summary>
        /// Get simple checksum for file name.
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public static string GetFileChecksum(string filePath)
        {
            using var fileStream = File.OpenRead(filePath);
            using var sha256 = SHA256.Create();

            byte[] hashBytes = sha256.ComputeHash(fileStream);
            return BitConverter.ToString(hashBytes).Replace("-", "").ToLower();
        }

        /// <summary>
        /// Delete directory.
        /// </summary>
        /// <param name="directoryLocation"></param>
        public static void DeleteDirectory(string directoryLocation)
        {
            if (Directory.Exists(directoryLocation))
            {
                Directory.Delete(directoryLocation, true);
            }
        }

        /// <summary>
        /// Delete file.
        /// </summary>
        /// <param name="fileLocation"></param>
        public static void DeleteFile(string fileLocation)
        {
            if (File.Exists(fileLocation))
            {
                File.Delete(fileLocation);
            }
        }

    }
}
