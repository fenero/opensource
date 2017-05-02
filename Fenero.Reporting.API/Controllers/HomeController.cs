using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Mvc.Ajax;
using System.Net;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Web.Script.Serialization;
using System.Text;
using System.Configuration;

namespace Fenero.Reporting.API.Controllers
{
	public class HomeController : Controller
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

		public ActionResult Index ()
		{
			// hookup our date ranges
			DateTime startDate = DateTime.Parse ("2016-10-14 12:00");
			DateTime endDate = DateTime.Parse ("2016-10-14 13:00");

			// in production, may want to use the following to pull the last 5-10 mins of real-time data
			//startDate = DateTime.Now.AddMinutes(-10.0);
			//endDate = DateTime.Now.AddMinutes(-5.0);

			// used to store comma-seperated list of active programs
			List<string> campaignIds = new List<string>();
			List<string> listIds = new List<string>();
			List<string> queueIds = new List<string>();
			List<string> userIds = new List<string>();

			List<Program> runningPrograms = GetActivePrograms (startDate, endDate);

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

			SavePrograms (runningPrograms);

			// now process for inbound queue data
			if (queueIds.Count () > 0) {
				ProcessSystemCallLog (startDate, endDate, string.Join(",", queueIds), string.Join(",", campaignIds));
			}

			// process calls to API for list data
			if (listIds.Count () > 0) {
				ProcessInteractionDetail (startDate, endDate, string.Join(",", listIds), string.Join(",", campaignIds));
			}

			// process calls to API for agent data
			if (userIds.Count () > 0) {
				ProcessAgentLog (startDate, endDate, string.Join (",", campaignIds), string.Join (",", userIds));
			}

			return View ();
		}

		/// <summary>
		/// Gets the active campaigns queues and lists.
		/// </summary>
		/// <returns>The active campaigns queues and lists.</returns>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		public List<Program> GetActivePrograms (DateTime startDate, DateTime endDate)
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

		public void SavePrograms (List<Program> programs)
		{
			using (WebClient client = new WebClient())
			{
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

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_Programs", dt, con);

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_Programs";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();
			}
		}

		/// <summary>
		/// Processes the interaction detail report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="listIds">List identifiers.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public void ProcessInteractionDetail (DateTime startDate, DateTime endDate, string listIds, string campaignIds)
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

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

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

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_InteractionDetail", dt, con);

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_InteractionDetail";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();
			}
		}

		/// <summary>
		/// Processes the interaction detail report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="listIds">List identifiers.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public void ProcessSystemCallLog (DateTime startDate, DateTime endDate, string queueIds, string campaignIds)
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

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

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

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_CallLog", dt, con);

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_CallLog";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();
			}
		}

		/// <summary>
		/// Processes the agent log report for the given params.
		/// </summary>
		/// <param name="startDate">Start date.</param>
		/// <param name="endDate">End date.</param>
		/// <param name="campaignIds">Campaign identifiers.</param>
		public void ProcessAgentLog (DateTime startDate, DateTime endDate, string campaignIds, string userIds)
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

				// get results into stream for processing
				Stream stream = client.OpenRead (apiUri.ToString ());
				StreamReader sr = new StreamReader(stream);

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

				string conn = ConfigurationManager.ConnectionStrings["FeneroStaging"].ToString();

				SqlConnection con = new SqlConnection(conn);
				con.Open();

				CheckIfTableExists ("Fenero_AgentLog", dt, con);

				// move data from data table into DB
				SqlBulkCopy bc = new SqlBulkCopy(con.ConnectionString, SqlBulkCopyOptions.TableLock);
				bc.DestinationTableName = "Fenero_AgentLog";
				bc.BatchSize = dt.Rows.Count;
				bc.WriteToServer(dt);
				bc.Close();
				con.Close();
			}
		}

		private void CheckIfTableExists(string tableName, DataTable dt, SqlConnection con){
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
		private string GetTimeZoneOffset(){

			bool isDaylightSavings = false;
			string tzOffset = "-5.00";

			try
			{
				TimeZoneInfo tz = TimeZoneInfo.FindSystemTimeZoneById ("America/New_York");
				isDaylightSavings = tz.IsDaylightSavingTime (DateTime.Now);
			}
			catch(Exception e)
			{
				TimeZoneInfo tz = TimeZoneInfo.FindSystemTimeZoneById ("Eastern Standard Time");
				isDaylightSavings = tz.IsDaylightSavingTime (DateTime.Now);
				Response.Write (e.ToString ());
			}

			if (isDaylightSavings)
				tzOffset = "-4.00";

			return tzOffset;
		}
	}
}
