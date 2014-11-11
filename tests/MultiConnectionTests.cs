using System;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Npgsql;
using NUnit.Framework;

namespace NpgsqlTests
{
	[TestFixture]
	public class MultiConnectionTests : TestBase
	{
		public MultiConnectionTests(string backendVersion) : base(backendVersion) { }

		private class RequestHandle
		{
			public static readonly object syncRoot = new object();
			public int successfulQueries, failedQueries;
			public TimeSpan minElapsedTime = TimeSpan.MaxValue, maxElapsedTime = TimeSpan.Zero, totalElapsedTime = TimeSpan.Zero;
		}

		[Test]
		public void OpenMultipleConnections([Values(2, 4, 8, 16, 32, 64)]int connectionCount)
		{
			Action<WaitCallback> enqueue = callback => ThreadPool.QueueUserWorkItem(callback);
			var startConnectionCount = GetOpenConnections();
			Console.WriteLine("Starting with ConnectionCount: {0}", startConnectionCount);
			var reqHandle = new RequestHandle();

			for (int i = 0; i < connectionCount; i++)
				enqueue(obj => ExecuteRequest(reqHandle));
			bool loop = true;
			int retry = 3;
			var sw = Stopwatch.StartNew();
			while (loop)
			{
				var currentConnectionCount = GetOpenConnections();
				Console.WriteLine("\r\nCurrent ConnectionCount: {0}", currentConnectionCount);
				Console.WriteLine("Successful Connections: {0}", reqHandle.successfulQueries);
				Console.WriteLine("Failing Connections: {0}", reqHandle.failedQueries);

				if (sw.Elapsed.TotalSeconds > connectionCount * 2)
					Assert.Fail("Could not start all queries in safe amount of time");
				else if (reqHandle.failedQueries + reqHandle.successfulQueries == connectionCount)
				{
					if (retry > 0)
					{
						retry--;
						Thread.Sleep(500);
					}
					else
						loop = false;
				}
				else
				{
					Thread.Sleep(500);
				}
			}
			Console.WriteLine("Elapsed time: {0:n}", sw.Elapsed.TotalSeconds);
			if (reqHandle.failedQueries > 0)
				Assert.Fail("Some queries (count: {0}) have failed", reqHandle.failedQueries);
			else
				Assert.Pass("All queries passed");
		}

		void ExecuteRequest(RequestHandle requestHandle)
		{
			Action execute = () =>
			{
				const string QuerySelect = "select 42 as result;";
				const string QuerySleep = "SELECT pg_sleep(1);";
				using (var connection = new NpgsqlConnection(ConnectionString + ";MaxPoolSize=0;MinPoolSize=15"))
				{
					connection.Open();

					using (var command = connection.CreateCommand())
					{
						command.CommandText = QuerySleep;
						command.ExecuteNonQuery();

						command.CommandText = QuerySelect;
						command.CommandType = CommandType.Text;

						//command.Prepare();

						var result = (int)command.ExecuteScalar();
						Assert.That(result, Is.EqualTo(42), "ERROR!");
					}
				}
			};
			try
			{
				var timer = Stopwatch.StartNew();
				execute();
				var elapsedTime = timer.Elapsed;

				lock (RequestHandle.syncRoot)
				{
					requestHandle.successfulQueries++;
					if (elapsedTime < requestHandle.minElapsedTime) requestHandle.minElapsedTime = elapsedTime;
					if (elapsedTime > requestHandle.maxElapsedTime) requestHandle.maxElapsedTime = elapsedTime;
					requestHandle.totalElapsedTime += elapsedTime;
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
				lock (RequestHandle.syncRoot) requestHandle.failedQueries++;
			}
		}

		int GetOpenConnections(int port = 5432, int? pid = null)
		{
			using (var process = new Process())
			{
				process.StartInfo = new ProcessStartInfo
				{
					FileName = @"C:\windows\system32\netstat.exe",
					Arguments = "-n -o",
					UseShellExecute = false,
					RedirectStandardOutput = true,
					CreateNoWindow = true,
				};

				process.Start();

				var output = process.StandardOutput.ReadToEnd();

				var connections = output
					.Split('\n')
					.Skip(4)
					.Select(x => x.Trim())
					.Select(x => x.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
					.Where(x => x.Length >= 5)
					.Select(x => new { proto = x[0], local = x[1], remote = x[2], state = x[3], pid = int.Parse(x[4]) })
					.ToList();

				var myConnections = connections
					.Where(x => x.proto == "TCP" && (!pid.HasValue || (x.pid == pid.Value)) && x.remote.EndsWith(":" + port))
					.ToList();

				process.WaitForExit();

				return myConnections.Count;
			}


		}
	}
}
