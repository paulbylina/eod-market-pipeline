#region Using declarations
using System;
using System.Globalization;
using System.IO;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.NinjaScript.Indicators
{
    public class TickRecorder : Indicator
    {
        private string outputPath;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Description = "Records raw NinjaTrader L1 Bid/Ask/Last events to CSV.";
                Name = "Tick Recorder";
                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                DisplayInDataBox = false;
                DrawOnPricePanel = true;
                DrawHorizontalGridLines = true;
                DrawVerticalGridLines = true;
                PaintPriceMarkers = false;
                IsSuspendedWhileInactive = false;
            }
            else if (State == State.DataLoaded)
            {
                string workspaceRoot = @"C:\REPOSITORY\trading-dev-framework";

                string instrumentFolder = SanitizePathPart(Instrument.FullName);
                DateTime sessionStartUtc = DateTime.UtcNow;
                TimeZoneInfo centralTz = TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time");
                DateTime sessionStartCentral = TimeZoneInfo.ConvertTimeFromUtc(sessionStartUtc, centralTz);

                string folder = Path.Combine(
                    workspaceRoot,
                    "ninja-lake",
                    "raw",
                    "ninjatrader",
                    "L1",
                    instrumentFolder,
                    $"{sessionStartCentral:yyyy-MM-dd}"
                );

                Directory.CreateDirectory(folder);

                string fileName = $"session_{sessionStartCentral:HHmmss_fff}_CT.csv";
                outputPath = Path.Combine(folder, fileName);

                WriteLine("ts_utc,instrument,market_data_type,price,size");
            }
        }

        protected override void OnBarUpdate()
        {
        }

        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (string.IsNullOrEmpty(outputPath))
                return;

            if (e.MarketDataType != MarketDataType.Bid &&
                e.MarketDataType != MarketDataType.Ask &&
                e.MarketDataType != MarketDataType.Last)
                return;

            string tsUtc = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            string instrumentName = Instrument.FullName;
            string marketDataType = e.MarketDataType.ToString();
            string price = e.Price.ToString(CultureInfo.InvariantCulture);
            string size = e.Volume.ToString(CultureInfo.InvariantCulture);

            WriteLine($"{tsUtc},{instrumentName},{marketDataType},{price},{size}");
        }

        private void WriteLine(string line)
        {
            File.AppendAllText(outputPath, line + Environment.NewLine);
        }

        private string SanitizePathPart(string value)
        {
            foreach (char c in Path.GetInvalidFileNameChars())
                value = value.Replace(c, '_');

            return value.Replace(' ', '_');
        }
    }
}

#region NinjaScript generated code. Neither change nor remove.

namespace NinjaTrader.NinjaScript.Indicators
{
	public partial class Indicator : NinjaTrader.Gui.NinjaScript.IndicatorRenderBase
	{
		private TickRecorder[] cacheTickRecorder;
		public TickRecorder TickRecorder()
		{
			return TickRecorder(Input);
		}

		public TickRecorder TickRecorder(ISeries<double> input)
		{
			if (cacheTickRecorder != null)
				for (int idx = 0; idx < cacheTickRecorder.Length; idx++)
					if (cacheTickRecorder[idx] != null &&  cacheTickRecorder[idx].EqualsInput(input))
						return cacheTickRecorder[idx];
			return CacheIndicator<TickRecorder>(new TickRecorder(), input, ref cacheTickRecorder);
		}
	}
}

namespace NinjaTrader.NinjaScript.MarketAnalyzerColumns
{
	public partial class MarketAnalyzerColumn : MarketAnalyzerColumnBase
	{
		public Indicators.TickRecorder TickRecorder()
		{
			return indicator.TickRecorder(Input);
		}

		public Indicators.TickRecorder TickRecorder(ISeries<double> input )
		{
			return indicator.TickRecorder(input);
		}
	}
}

namespace NinjaTrader.NinjaScript.Strategies
{
	public partial class Strategy : NinjaTrader.Gui.NinjaScript.StrategyRenderBase
	{
		public Indicators.TickRecorder TickRecorder()
		{
			return indicator.TickRecorder(Input);
		}

		public Indicators.TickRecorder TickRecorder(ISeries<double> input )
		{
			return indicator.TickRecorder(input);
		}
	}
}

#endregion
