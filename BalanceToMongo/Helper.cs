using System;
using System.Linq;
using System.Text;
using Neo;

namespace BalanceToMongo
{
    public static class Helper
    {
        public static string HexToString(this string hex)
        {
            // Console.WriteLine("HexToString {0} ",hex);
            if (hex.Length % 2 != 0)
            {
                throw new ArgumentException();
            }
            var output = "";
            for (int i = 0; i <= hex.Length - 2; i += 2)
            {
                try
                {
                    var result = Convert.ToByte(new string(hex.Skip(i).Take(2).ToArray()), 16);
                    output += (Convert.ToChar(result));
                }
                catch (Exception)
                {
                    throw;
                }
            }
            return output;
        }
        public static string ToHexString(this string str)
        {
            byte[] byteArray = Encoding.Default.GetBytes(str.ToCharArray());
            return byteArray.ToHexString();
        }
    }
}