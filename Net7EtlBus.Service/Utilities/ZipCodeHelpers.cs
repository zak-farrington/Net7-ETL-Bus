namespace Net7EtlBus.Service.Utilities
{
    public static class ZipCodeHelpers
    {
        /// <summary>
        /// Get standardized composite key for Zip Code to StateCode combinations.
        /// </summary>
        /// <param name="zipCode"></param>
        /// <param name="stateCode"></param>
        /// <returns></returns>
        public static string GetCompositeKey(string zipCode, string stateCode)
        {
            return $"{zipCode}_{stateCode}";
        }
    }
}
