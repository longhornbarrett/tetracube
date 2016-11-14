using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace HomeworkTask01
{
    /// <summary>
    /// Naive solution
    /// </summary>
    public class Task01_Naive1
    {
        private static DateTime epochDate = new DateTime(1970, 1, 1);

        private static string ToDateEpochString(string dateString)
        {
            int year = int.Parse(dateString.Substring(0, 4));
            int month = int.Parse(dateString.Substring(4, 2));
            int date = int.Parse(dateString.Substring(6, 2));
            int hour = int.Parse(dateString.Substring(8, 2));
            int minute = int.Parse(dateString.Substring(10, 2));
            int second = int.Parse(dateString.Substring(12, 2));
            int us = int.Parse(dateString.Substring(14, 6));
            DateTime dt = new DateTime(year, month, date, hour, minute, second);
            string result = (dt - epochDate).TotalSeconds + us.ToString("000000");
            return result;
        }

        private static string ComputeLimitRangeString(string lowPrice, string highPrice)
        {
            if (lowPrice == null || highPrice == null) return "NULL";
            if (lowPrice[lowPrice.Length - 8] != '.') throw new Exception("Bug");
            if (highPrice[highPrice.Length - 8] != '.') throw new Exception("Bug");
            lowPrice = lowPrice.Replace(".", "");
            highPrice = highPrice.Replace(".", "");
            long lLowPrice = long.Parse(lowPrice);
            long lHighPrice = long.Parse(highPrice);
            long lRange = lHighPrice - lLowPrice;
            string sRange = lRange.ToString("00000000");
            string result = sRange.Substring(0, sRange.Length - 7) + "." + sRange.Substring(sRange.Length - 7, 7);
            return result;
        }

        public static void ParseLine(string line, StringBuilder sb)
        {
            if (string.IsNullOrWhiteSpace(line)) return;
            string tag48 = null;
            string tag55 = null;
            string tag779 = null;
            string tag1148 = null;
            string tag1149 = null;
            string tag1150 = null;
            string[] fields = line.Split((char)0x01);
            foreach (string field in fields)
            {
                if (string.IsNullOrWhiteSpace(field)) continue;
                string[] tv = field.Split('=');
                if (tv.Length != 2) continue;
                string tagString = tv[0];
                string valueString = tv[1];
                switch (tagString)
                {
                    case "48": tag48 = valueString; break;
                    case "55": tag55 = valueString; break;
                    case "779": tag779 = valueString; break;
                    case "1148": tag1148 = valueString; break;
                    case "1149": tag1149 = valueString; break;
                    case "1150": tag1150 = valueString; break;
                }
            }
            sb.Append($"{tag48}:{tag55}\n");
            sb.Append($"\tLastUpdateTime={ToDateEpochString(tag779)}\n");
            sb.Append($"\tLowLimitPrice={tag1148 ?? "NULL"}\n");
            sb.Append($"\tHighLimitPrice={tag1149 ?? "NULL"}\n");
            sb.Append($"\tLimitPriceRange={ComputeLimitRangeString(tag1148, tag1149)}\n");
            sb.Append($"\tTradingReferencePrice={tag1150 ?? "NULL"}\n");
        }

        public static void Parse(string inputPath, string outputPath)
        {
            StringBuilder sb = new StringBuilder();
            string rawInput = File.ReadAllText(inputPath);
            foreach (string line in rawInput.Split('\n'))
            {
                ParseLine(line, sb);
            }
            File.WriteAllText(outputPath, sb.ToString());
        }

        static void Main(string[] args)
        {
            string inputPath;
            string outputPath;
            if (args.Length == 0)
            {
                inputPath = "secdef.dat";
                outputPath = "secdef_parsed.txt";
            }
            else
            {
                inputPath = args[0];
                outputPath = args[1];
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            Parse(inputPath, outputPath);
            stopwatch.Stop();
            Console.WriteLine($"Code executed in {stopwatch.ElapsedMilliseconds} ms");
        }
    }
}
