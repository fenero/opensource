using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Net;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.Data;
using System.IO;
//using System.Web.Script.Serialization;
using System.Text;
using System.Configuration;
using System.Web.Script.Serialization;
using System.Diagnostics;
using System.Reflection;

namespace Fenero.Reporting.API
{
	class MainClass
	{
		/// <summary>
		/// Base URL for all API calls
		/// </summary>
		const string BASE_API_URL = "https://api.fenero.com/MobileApi";

		/// <summary>
		/// Go to the Users app in Fenero Manager and create an API user.
		/// Use the "Fenero Id" column in the search results in the constant below.
		/// </summary>
		const string API_USER_ID = "1234";

		/// <summary>
		/// Go to the Fenero Manager's Launchpad (where we show all apps) and
		/// hover over your name. In the dropdown menu, click on the FTP/API
		/// tab and click the "Regenerate Token" to create an API token.
		/// 
		/// Enter the value in the constant beow.
		/// </summary>
		const string API_KEY = "aaaabbbbccccddddeeeeffffgggg";

		/// <summary>
		/// Object used to map our return list of active programs.
		/// </summary>
		public class Program
		{
			public string ID { get; set; }
			public string DisplayName { get; set; }
			public string Type { get; set; }
			public string IsActive { get; set; }
		}

		public sealed class ProgramType {

			private readonly String name;

			public static readonly ProgramType ACDQueue = new ProgramType ("ACDQueue");
			public static readonly ProgramType Campaign = new ProgramType ("Campaign");
			public static readonly ProgramType DialingList = new ProgramType ("DialingList");
			public static readonly ProgramType Agent = new ProgramType ("Agent");

			private ProgramType (String name){
				this.name = name;
			}

			public override String ToString(){
				return name;
			}
		}

		public static void Main (string[] args)
		{
			// hookup our date ranges
			DateTime startDate = DateTime.Parse ("2016-10-14 12:00");
			DateTime endDate = DateTime.Parse ("2016-10-14 13:00");

			// in production, may want to use the following
			//startDate = DateTime.Now.AddMinutes(-10.0);
			//endDate = DateTime.Now.AddMinutes(-5.0);

			// used to store comma-seperated list of active programs
			List<string> campaignIds = new List<string>();
			List<string> listIds = new List<string>();
			List<string> queueIds = new List<string>();
			List<string> userIds = new List<string>();

			#if DEBUG
			Console.WriteLine (string.Format ("{0} Preparing to fetch the list of active programs for dates {1} to {2}", DateTime.Now, startDate, endDate));
			DateTime debugStartTime = DateTime.Now;
			#endif

			List<Program> runningPrograms = GetActivePrograms (startDate, endDate);

			#if DEBUG
			TimeSpan debugEndTime = DateTime.Now.Subtract (debugStartTime);
			Console.WriteLine (string.Format ("{0} Fetching list of active programs took {1} second(s) to execute...", DateTime.Now, debugEndTime.TotalSeconds));
			debugStartTime = DateTime.Now;
			#endif

			// retrieve our list of active programs for the specified timeframe
			foreach (Program program in runningPrograms) {
				if (program.Type == ProgramType.ACDQueue.ToString()) {
					queueIds.Add (program.ID);
				} else if (program.Type == ProgramType.Campaign.ToString()) {
					campaignIds.Add (program.ID);
				} else if (program.Type == ProgramType.DialingList.ToString()) {
					listIds.Add (program.ID);
				} else if (program.Type == ProgramType.Agent.ToString()) {
					userIds.Add (program.ID);
				}
			}

			#if DEBUG
			Console.WriteLine (string.Format ("{0} Preparing to save programs output to SQL...", DateTime.Now));
			debugStartTime = DateTime.Now;
			#endif

			SavePrograms (runningPrograms);

			// now process for inbound queue data
			if (queueIds.Count () > 0) {
				#if DEBUG
				Console.WriteLine (string.Format ("{0} We have {1} queues to process. Fetching API data for these queues now...", DateTime.Now, queueIds.Count()));
				debugStartTime = DateTime.Now;
				#endif

				ProcessSystemCallLog (startDate, endDate, string.Join(",", queueIds), string.Join(",", campaignIds));

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Fetching queues data took {1} second(s) to execute...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif
			}

			// process calls to API for list data
			if (listIds.Count () > 0) {
				#if DEBUG
				Console.WriteLine (string.Format ("{0} We have {1} dialing lists to process. Fetching API data for these lists now...", DateTime.Now, listIds.Count()));
				debugStartTime = DateTime.Now;
				#endif

				ProcessInteractionDetail (startDate, endDate, string.Join(",", listIds), string.Join(",", campaignIds));

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Fetching dialing lists data took {1} second(s) to execute...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif
			}

			// process calls to API for agent data
			if (userIds.Count () > 0) {
				#if DEBUG
				Console.WriteLine (string.Format ("{0} We have {1} agents to process. Fetching API data for these agents now...", DateTime.Now, userIds.Count()));
				debugStartTime = DateTime.Now;
				#endif

				ProcessAgentLog (startDate, endDate, string.Join (",", campaignIds), string.Join (",", userIds));

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Fetching agents data took {1} second(s) to execute...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif
			}

		}

		/// <summary>
		/// Gets the active campaigns queues and lists.
		/// </summary>
		/// <returns>The active campaigns queues and lists.</returns>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		public static List<Program> GetActivePrograms (DateTime startDate, DateTime endDate)
		{
			using (WebClient client = new WebClient())
			{
				byte[] response =
					client.UploadValues (BASE_API_URL + "/GetActivePrograms", new NameValueCollection () {
						{ "startDate", startDate.ToString () },
						{ "endDate", endDate.ToString () },
						{ "userId", API_USER_ID },
						{ "tzOffset", GetTimeZoneOffset() },
						{ "appTokenId", API_KEY }
					});

				var serializer = new JavaScriptSerializer() { MaxJsonLength = int.MaxValue };

				dynamic results = serializer.Deserialize<List<Program>> (System.Text.Encoding.UTF8.GetString (response));
				return results;
			}
		}

		public static void SavePrograms (List<Program> programs)
		{

			#if DEBUG
			Console.WriteLine (string.Format ("{0} Loading programs data into DataTable...", DateTime.Now));
			DateTime debugStartTime = DateTime.Now;
			#endif

			string[] columns = new string []{ "ID", "DisplayName", "Type", "IsActive" };
			DataTable dt = new DataTable();
			DataRow row;
			foreach (string dc in columns)
			{
				dt.Columns.Add(new DataColumn(dc));
			}
			foreach (Program p in programs)
			{
				row = dt.NewRow();
				row ["ID"] = p.ID;
				row ["DisplayName"] = p.DisplayName;
				row ["Type"] = p.Type;
				row ["IsActive"] = p.IsActive;
				dt.Rows.Add(row);
			}

			#if DEBUG
			TimeSpan debugEndTime = DateTime.Now.Subtract (debugStartTime);
			Console.WriteLine (string.Format ("{0} Loading programs DataTable took {1} second(s) to execute. It has {2} rows. Verifying if Fenero_Programs table exists in SQL...", DateTime.Now, debugEndTime.TotalSeconds, dt.Rows.Count));
			debugStartTime = DateTime.Now;
			#endif

			string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

			SqlConnection con = new SqlConnection(conn);
			con.Open();

			CheckIfTableExists ("Fenero_Programs", dt, con);

			#if DEBUG
			debugEndTime = DateTime.Now.Subtract (debugStartTime);
			Console.WriteLine (string.Format ("{0} Done checking if programs SQL table exists. Preparing to WriteToServer...", DateTime.Now));
			debugStartTime = DateTime.Now;
			#endif

			// move data from data table into DB
			SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
			bc.DestinationTableName = "Fenero_Programs";
			bc.BatchSize = dt.Rows.Count;
			bc.WriteToServer(dt);
			bc.Close();
			con.Close();

			#if DEBUG
			debugEndTime = DateTime.Now.Subtract (debugStartTime);
			Console.WriteLine (string.Format ("{0} WriteToServer for Fenero_Programs completed.", DateTime.Now));
			debugStartTime = DateTime.Now;
			#endif

		}

		/// <summary>
		/// Processes the interaction detail report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="listIds">List identifiers.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public static void ProcessInteractionDetail (DateTime startDate, DateTime endDate, string listIds, string campaignIds)
		{
			using (WebClient client = new WebClient())
			{
				StringBuilder apiUri = new StringBuilder (BASE_API_URL + "/ReportInteractionDetailLog?");
				apiUri.AppendFormat ("userId={0}", API_USER_ID);
				apiUri.AppendFormat ("&appTokenId={0}", API_KEY);
				apiUri.AppendFormat ("&startDate={0}", startDate.ToString());
				apiUri.AppendFormat ("&endDate={0}", endDate.ToString());
				apiUri.AppendFormat ("&listIds={0}", listIds);
				apiUri.AppendFormat ("&campaignIds={0}", campaignIds);
				apiUri.AppendFormat ("&dispositionIds={0}", string.Empty);	// specify to further filter by dispos
				apiUri.AppendFormat ("&users={0}", string.Empty);			// specify to further filter by users
				apiUri.AppendFormat ("&tzOffset={0}", GetTimeZoneOffset());

				#if DEBUG
				Console.WriteLine (string.Format ("{0} Sending off API call to {1}...", DateTime.Now, apiUri.ToString()));
				DateTime debugStartTime = DateTime.Now;
				#endif

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

				#if DEBUG
				TimeSpan debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} API call returned after {1} second(s). Loading interaction detail data into DataTable...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif

				// move from stream into database for BC import
				string line = sr.ReadLine();
				string[] columns = line.Split(',');
				DataTable dt = new DataTable();
				DataRow row;
				foreach (string dc in columns)
				{
					dt.Columns.Add(new DataColumn(dc));
				}
				while (!sr.EndOfStream)
				{
					columns = sr.ReadLine().Split(',');
					if (columns.Length == dt.Columns.Count)
					{
						row = dt.NewRow();
						row.ItemArray = columns;
						dt.Rows.Add(row);
					}
				}

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Loading interaction detail DataTable took {1} second(s) to execute. It has {2} rows. Verifying if Fenero_InteractionDetail table exists in SQL...", DateTime.Now, debugEndTime.TotalSeconds, dt.Rows.Count));
				debugStartTime = DateTime.Now;
				#endif

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_InteractionDetail", dt, con);

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Done checking if interaction detail SQL table exists. Preparing to WriteToServer...", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_InteractionDetail";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} WriteToServer for Fenero_InteractionDetail completed.", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif
			}
		}

		/// <summary>
		/// Processes the interaction detail report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="listIds">List identifiers.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public static void ProcessSystemCallLog (DateTime startDate, DateTime endDate, string queueIds, string campaignIds)
		{
			using (WebClient client = new WebClient())
			{
				StringBuilder apiUri = new StringBuilder (BASE_API_URL + "/ReportSystemCallLog?");
				apiUri.AppendFormat ("userId={0}", API_USER_ID);
				apiUri.AppendFormat ("&appTokenId={0}", API_KEY);
				apiUri.AppendFormat ("&startDate={0}", startDate.ToString());
				apiUri.AppendFormat ("&endDate={0}", endDate.ToString());
				apiUri.AppendFormat ("&queueIds={0}", queueIds);
				apiUri.AppendFormat ("&campaignIds={0}", campaignIds); // supplying this provides log data for outbound
				apiUri.AppendFormat ("&tzOffset={0}", GetTimeZoneOffset());

				#if DEBUG
				Console.WriteLine (string.Format ("{0} Sending off API call to {1}...", DateTime.Now, apiUri.ToString()));
				DateTime debugStartTime = DateTime.Now;
				#endif

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

				#if DEBUG
				TimeSpan debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} API call returned after {1} second(s). Loading system call log data into DataTable...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif

				// move from stream into database for BC import
				string line = sr.ReadLine();
				string[] columns = line.Split(',');
				DataTable dt = new DataTable();
				DataRow row;
				foreach (string dc in columns)
				{
					dt.Columns.Add(new DataColumn(dc));
				}
				while (!sr.EndOfStream)
				{
					columns = sr.ReadLine().Split(',');
					if (columns.Length == dt.Columns.Count)
					{
						row = dt.NewRow();
						row.ItemArray = columns;
						dt.Rows.Add(row);
					}
				}

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Loading system call log DataTable took {1} second(s) to execute. It has {2} rows. Verifying if Fenero_CallLog table exists in SQL...", DateTime.Now, debugEndTime.TotalSeconds, dt.Rows.Count));
				debugStartTime = DateTime.Now;
				#endif

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_CallLog", dt, con);

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Done checking if system call log SQL table exists. Preparing to WriteToServer...", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_CallLog";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} WriteToServer for Fenero_CallLog completed.", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif
			}
		}

		/// <summary>
		/// Processes the agent log report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public static void ProcessAgentLog (DateTime startDate, DateTime endDate, string campaignIds, string userIds)
		{
			using (WebClient client = new WebClient())
			{
				StringBuilder apiUri = new StringBuilder (BASE_API_URL + "/ReportAgentLog?");
				apiUri.AppendFormat ("userId={0}", API_USER_ID);
				apiUri.AppendFormat ("&appTokenId={0}", API_KEY);
				apiUri.AppendFormat ("&startDate={0}", startDate.ToString());
				apiUri.AppendFormat ("&endDate={0}", endDate.ToString());
				apiUri.AppendFormat ("&campaignIds={0}", campaignIds);
				apiUri.AppendFormat ("&users={0}", userIds);
				apiUri.AppendFormat ("&tzOffset={0}", GetTimeZoneOffset());

				#if DEBUG
				Console.WriteLine (string.Format ("{0} Sending off API call to {1}...", DateTime.Now, apiUri.ToString()));
				DateTime debugStartTime = DateTime.Now;
				#endif

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

				#if DEBUG
				TimeSpan debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} API call returned after {1} second(s). Loading agent log data into DataTable...", DateTime.Now, debugEndTime.TotalSeconds));
				debugStartTime = DateTime.Now;
				#endif

				// move from stream into database for BC import
				string line = sr.ReadLine();
				string[] columns = line.Split(',');
				DataTable dt = new DataTable();
				DataRow row;
				foreach (string dc in columns)
				{
					dt.Columns.Add(new DataColumn(dc));
				}
				while (!sr.EndOfStream)
				{
					columns = sr.ReadLine().Split(',');
					if (columns.Length == dt.Columns.Count)
					{
						row = dt.NewRow();
						row.ItemArray = columns;
						dt.Rows.Add(row);
					}
				}

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Loading agent log DataTable took {1} second(s) to execute. It has {2} rows. Verifying if Fenero_AgentLog table exists in SQL...", DateTime.Now, debugEndTime.TotalSeconds, dt.Rows.Count));
				debugStartTime = DateTime.Now;
				#endif

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_AgentLog", dt, con);

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} Done checking if agent log SQL table exists. Preparing to WriteToServer...", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_AgentLog";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();

				#if DEBUG
				debugEndTime = DateTime.Now.Subtract (debugStartTime);
				Console.WriteLine (string.Format ("{0} WriteToServer for Fenero_AgentLog completed.", DateTime.Now));
				debugStartTime = DateTime.Now;
				#endif
			}
		}

		private static void CheckIfTableExists(string tableName, DataTable dt, SqlConnection con){
			string createTable = string.Empty;
			createTable += "IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[" + tableName + "]') AND type in (N'U'))";
			createTable += "BEGIN ";
			createTable += "CREATE TABLE " + tableName + "";
			createTable += "(";
			for (int i = 0; i < dt.Columns.Count; i++)
			{
				if (i != dt.Columns.Count-1)
					createTable += "[" + dt.Columns[i].ColumnName + "] " + "varchar(max)" + ",";
				else
					createTable += "[" + dt.Columns[i].ColumnName + "] " + "varchar(max)";
			}
			createTable += ") ";
			createTable += "END ";
			createTable += "ELSE ";
			createTable += "TRUNCATE TABLE " + tableName;

			SqlCommand cmd = new SqlCommand();
			cmd.CommandType = CommandType.Text;
			cmd.CommandText = createTable;
			cmd.Connection = con;
			cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// Gets us the TZ offset for reports based on whether we're in EST or EDT
		/// </summary>
		private static string GetTimeZoneOffset(){

			bool isDaylightSavings = false;
			string tzOffset = "-5.00";

			try
			{
				TimeZoneInfo tz = TimeZoneInfo.FindSystemTimeZoneById ("America/New_York");
				isDaylightSavings = tz.IsDaylightSavingTime (DateTime.Now);
			}
			catch(Exception e)
			{
				try
				{
					TimeZoneInfo tz = TimeZoneInfo.FindSystemTimeZoneById ("Eastern Standard Time");
					isDaylightSavings = tz.IsDaylightSavingTime (DateTime.Now);
					Console.WriteLine (e.ToString ());
				}
				catch(Exception ex)
				{
					Console.WriteLine (ex.ToString ());
				}
			}

			if (isDaylightSavings)
				tzOffset = "-4.00";

			return tzOffset;
		}

		public static void TellMe(string message, params object[] values){
			Console.WriteLine (string.Format ("{0}:{1}", message, values));
		}
	}
}
