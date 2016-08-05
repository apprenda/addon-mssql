using Apprenda.SaaSGrid.Addons;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Data;
using Apprenda.Services.Logging;
using System.Data.SqlClient;

namespace Apprenda.MsSql.AddOn
{
    public class MSSQLAddOn : AddonBase
    {
        const string ConnectionStringFormatter = @"Data Source={0},{1};User Id={2};Password={3};Initial Catalog={4};";
        const string DatabaseNameFormatter = @"{0}__{1}";
        const string DatabaseUsernameFormatter = @"DB_{0}__{1}";

        private static readonly ILogger log = LogManager.Instance().GetLogger(typeof(MSSQLAddOn));
        
        static class Queries
        {
            public const string CreateUser = @"CREATE LOGIN {0} WITH PASSWORD = '{1}'; USE {2}; CREATE USER {0} FOR LOGIN {0};";
            public const string CreateDatabase = @"CREATE DATABASE {0};";
            public const string GrantAllPrivilegesToDatabase = @"USE {0}; GRANT ALL TO {1};";
            public const string DropDatabase = @"USE master; DROP DATABASE {0};";
            public const string DropUser = @"DROP LOGIN {0};";
        }

        static class Keys 
        {
            public const string Server = "mssqlServer";
            public const string Port = "mssqlServerPort";
            public const string AdminDatabase = "mssqlAdminDatabase";
            public const string AdminUser = "mssqlAdminUser";
            public const string AdminPassword = "mssqlAdminPassword";
        }

        private string Server { get; set; }
        private int Port { get; set; }
        private string AdminDatabase { get; set; }
        private string AdminUserId { get; set; }
        private string AdminPassword { get; set; }
        private string AdminConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, AdminUserId, AdminPassword, AdminDatabase); } }
        private string NewDatabase { get; set; }
        private string NewUserId { get; set; }
        private string NewPassword { get; set; }
        private string NewConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, NewUserId, NewPassword, NewDatabase); } }
        public override OperationResult Deprovision(AddonDeprovisionRequest request)
        {
            var result = new OperationResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);

                log.InfoFormat("Removing MsSql database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    DropUser(connection);

                    DropDatabase(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully removed a MsSql database.";

                log.InfoFormat("Successfully removed MsSql database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to remove MsSql database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override ProvisionAddOnResult Provision(AddonProvisionRequest request)
        {
            var result = new ProvisionAddOnResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating MsSql database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateDatabase(connection);

                    CreateUser(connection);

                    GrantPrivileges(connection);
                }

                result.IsSuccess = true;
                result.ConnectionData = NewConnectionString;
                result.EndUserMessage = "Successfully created a MsSql database.";

                log.InfoFormat("Successfully created MsSql database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.ConnectionData = "";
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create MsSql database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override OperationResult Test(AddonTestRequest request)
        {
            var result = new OperationResult();

            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating and removing MsSql database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateDatabase(connection);
                    
                    CreateUser(connection);            

                    GrantPrivileges(connection);

                    DropUser(connection);

                    DropDatabase(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully created and removed a MsSql database.";

                log.InfoFormat("Successfully created and removed MsSql database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create or remove MsSql database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        private void DropDatabase(SqlConnection connection)
        {
            var dropDatabaseCommand = connection.CreateCommand();
            dropDatabaseCommand.CommandText = string.Format(Queries.DropDatabase, NewDatabase);
            ExecuteCommand(dropDatabaseCommand);
        }

        private void DropUser(SqlConnection connection)
        {
            var dropUserCommand = connection.CreateCommand();
            dropUserCommand.CommandText = string.Format(Queries.DropUser, NewUserId);
            ExecuteCommand(dropUserCommand);
        }

        private void GrantPrivileges(SqlConnection connection)
        {
            var grantPrivilegesCommand = connection.CreateCommand();
            grantPrivilegesCommand.CommandText = string.Format(Queries.GrantAllPrivilegesToDatabase, NewDatabase, NewUserId);
            ExecuteCommand(grantPrivilegesCommand);
        }

        private void CreateDatabase(SqlConnection connection)
        {
            var createDatabaseCommand = connection.CreateCommand();
            createDatabaseCommand.CommandText = string.Format(Queries.CreateDatabase, NewDatabase, NewUserId);
            ExecuteCommand(createDatabaseCommand);
        }

        private void CreateUser(SqlConnection connection)
        {
            try
            {
                var createUserCommand = connection.CreateCommand();
                createUserCommand.CommandText = string.Format(Queries.CreateUser, NewUserId, NewPassword, NewDatabase);
                ExecuteCommand(createUserCommand);
            }
            catch (Exception ex)
            {
                log.ErrorFormat("Error creating user: {0}", ex.Message);
            }
        }

        private static void ExecuteCommand(SqlCommand command)
        {
            command.CommandType = CommandType.Text;
            command.ExecuteNonQuery();
        }

        private static string GetDatabaseName(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseNameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetNewUsername(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseUsernameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetPassword()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.Now;
            var inputString = now.ToLongTimeString() + "__" + guid.ToString();
            
            byte[] bytes = Encoding.Unicode.GetBytes(inputString);
            SHA256Managed sha256 = new SHA256Managed();
            byte[] hash = sha256.ComputeHash(bytes);
            string password = string.Empty;
            foreach (byte x in hash)
            {
                password += String.Format("{0:x2}", x);
            }
            return string.Format("_{0}_", password);
        }

        private SqlConnection GetConnection(List<AddonProperty> properties)
        {
            ParseProperties(properties);

            return GetConnection(AdminConnectionString);
        }

        private SqlConnection GetConnection(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        private void ParseProperties(List<AddonProperty> properties)
        {
            try
            {
                Server = properties.Find(p => p.Key == Keys.Server).Value;
                Port = int.Parse(properties.Find(p => p.Key == Keys.Port).Value);
                AdminUserId = properties.Find(p => p.Key == Keys.AdminUser).Value;
                AdminPassword = properties.Find(p => p.Key == Keys.AdminPassword).Value;
                AdminDatabase = properties.Find(p => p.Key == Keys.AdminDatabase).Value;
            }
            catch (Exception ex)
            {
                log.Error(ex.Message);
            }
        }

    }
}
