using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.DirectoryServices;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

namespace Group.Helpers
{
    public class HadoopConfiguration
    {
        #region Variable Declaration
        string configDirectory;
        string ip;
        const string hdfsSiteFile = "hdfs-site.xml";
        const string yarnSiteFile = "yarn-site.xml";
        const string coreSiteFile = "core-site.xml";
        const string mapredSiteFile = "mapred-site.xml";
        const int resourceManagerPort = 8088;
        const int securedResourceManagerPort = 8090;
        const int nameNodePort = 50070;
        const int securedNameNodePort = 50470;
        bool isSecured;
        Credential credential;
        #endregion

        #region Variable Initialization

        public HadoopConfiguration(string configIp)
        {
            string hadoopHome = Environment.GetEnvironmentVariable("HADOOP_HOME");
            configDirectory = hadoopHome + "\\etc\\hadoopclusterHA_" + configIp + "\\";
            ip = configIp;
            isSecured = false;
        }

        public HadoopConfiguration(string configIp, bool isSecuredCluster, Credential credentials)
        {
            string hadoopHome = Environment.GetEnvironmentVariable("HADOOP_HOME");
            configDirectory = hadoopHome + "\\etc\\hadoopSecured_" + configIp + "\\";
            ip = configIp;
            isSecured = true;
            credential = credentials;
            //Find out Host name of AD
            IPHostEntry ipHostEntry = Dns.GetHostEntry(credential.ActiveDirectoryIp);
            string hostName = ipHostEntry.HostName.ToString();
            string domainName = GetADDomainName(credential.Username, credential.Password, hostName);
            domainName = domainName.Replace("DC=", ".").Replace(",", string.Empty).TrimStart('.');
            credential.DomainName = domainName;
        }

        #endregion

        #region Configuration Files

        internal bool GenerateConfigFiles(bool deleteExisting)
        {
            if (deleteExisting)
            {
                if (Directory.Exists(configDirectory))
                    Directory.Delete(configDirectory, true);
                Directory.CreateDirectory(configDirectory);
                GenerateHdfsConfigurationXml();
                GenerateMapRedConfigurationXml();
                GenerateCoreConfigurationXml();
                GenerateYarnConfigurationXml();
            }
            else
            {
                //ToDo
            }
            return true;
        }

        private bool CreateConfigFile(string rawData, string fileName)
        {
            if (!string.IsNullOrEmpty(rawData))
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(rawData);
                // Get all xml nodes from property tag
                XmlNodeList items = doc.GetElementsByTagName("property");
                // As only property nodes are received need to add configuration tag manually
                File.WriteAllText(configDirectory + fileName, "<configuration>\n");
                // source tag contains name of configuration file
                // for each node check source tag is present and also its text is corresponding file name
                foreach (XmlNode node in items)
                {
                    if (node.HasChildNodes && node["source"] != null && (node["source"].InnerText == fileName || node["source"].InnerText == "programatically"))
                    {
                        File.AppendAllText(configDirectory + fileName, node.OuterXml + "\n");
                    }
                }
                // As only property nodes are received need to add configuration tag manually
                File.AppendAllText(configDirectory + fileName, "</configuration>\n");
                if (fileName == "core-site.xml" && isSecured)
                {
                    var document = XDocument.Load(configDirectory + fileName);
                    var replacepropery = document.Elements("configuration").Elements("property").Elements("name")
                            .Single(name => name.Value == "hadoop.sync.username").Parent.Element("value");
                    replacepropery.Value = credential.Username + "@" + credential.DomainName;
                    foreach (var childElement in document.Root.Elements("property"))
                    {
                        if (childElement.Element("name").Value.Contains("hadoop.security.group.mapping.ldap.bind.password.file"))
                            childElement.Remove();
                    }
                    document.Save(configDirectory + fileName);
                }
                return true;
            }
            return false;
        }

        private bool GenerateHdfsConfigurationXml()
        {
            string resp;
            // hdfs site configurations are available in 50070/ conf

            if (isSecured)
            {
                string surl = "https://" + Dns.GetHostEntry(ip).HostName.ToLower() + ":" + securedNameNodePort + "/conf";
                string response = GetSecuredWebRequestAndResponse(surl, credential);
                return CreateConfigFile(response, hdfsSiteFile);
            }
            else
            {
                string url = "http://" + ip + ":" + nameNodePort + "/conf";
                if (TryGetResponse(url, out resp))
                {
                    return CreateConfigFile(resp, hdfsSiteFile);
                }
                else
                {
                    throw (new ConfigFileCreationFailed("\nFailed to create " + hdfsSiteFile) { UnresolvedUrl = url });
                }
            }
        }

        private bool GenerateYarnConfigurationXml()
        {
            string resp;
            string response;
            if (isSecured)
            {
                string surl = "https://" + Dns.GetHostEntry(ip).HostName.ToLower() + ":" + securedResourceManagerPort + "/ws/v1/cluster/nodes";
                response = GetSecuredWebRequestAndResponse(surl, credential);
                string nodeManagerIp = GetNodeManagerAddress(response);
                if (string.IsNullOrEmpty(nodeManagerIp))
                {
                    string sResourceMangerUrl = "https://" + Dns.GetHostEntry(ip).HostName.ToLower() + ":" + securedResourceManagerPort + "/ws/v1/cluster/nodes";
                    WebHeaderCollection data = GetSecuredWebRequestAndResponseHeader(sResourceMangerUrl, credential);
                    nodeManagerIp = TryGetRedirectedNodeManager(data);
                }
                string url = "https://" + nodeManagerIp + "/conf";
                response = GetSecuredWebRequestAndResponse(url, credential);
                return CreateConfigFile(response, yarnSiteFile); ;
            }
            else
            {
                TryGetResponse("http://" + ip + ":" + resourceManagerPort + "/ws/v1/cluster/nodes", out response);
                string nodeManagerIp = GetNodeManagerAddress(response);
                // If node manager cannot received if ip given is standby RM, so try to get node manager ip if redirection link exist
                if (string.IsNullOrEmpty(nodeManagerIp))
                {
                    string resourceManagerUrl = "http://" + ip + ":" + resourceManagerPort + "/ws/v1/cluster/nodes";
                    WebRequest request = WebRequest.Create(resourceManagerUrl);
                    // Gets Response headers of given Url
                    WebHeaderCollection headers = request.GetResponse().Headers;
                    nodeManagerIp = TryGetRedirectedNodeManager(headers);
                }
                string url = "http://" + nodeManagerIp + "/conf";
                if (TryGetResponse(url, out resp))
                {
                    return CreateConfigFile(resp, yarnSiteFile);
                }
                else
                {
                    throw (new ConfigFileCreationFailed("\nFailed to create " + yarnSiteFile) { UnresolvedUrl = url });
                }
            }
        }

        private bool GenerateMapRedConfigurationXml()
        {
            string resp;
            if (isSecured)
            {
                string url = "https://" + Dns.GetHostEntry(ip).HostName.ToLower() + ":" + securedResourceManagerPort + "/conf";
                resp = GetSecuredWebRequestAndResponse(url, credential);
                return CreateConfigFile(resp, mapredSiteFile);
            }
            else
            {
                string url = "http://" + ip + ":" + resourceManagerPort + "/conf";
                if (TryGetResponse(url, out resp))
                {
                    return CreateConfigFile(resp, mapredSiteFile);
                }
                else
                {
                    throw (new ConfigFileCreationFailed("\nFailed to create " + mapredSiteFile) { UnresolvedUrl = url });
                }
            }
        }

        private bool GenerateCoreConfigurationXml()
        {
            string resp;
            if (isSecured)
            {
                string url = "https://" + Dns.GetHostEntry(ip).HostName.ToLower() + ":" + securedResourceManagerPort + "/conf";
                resp = GetSecuredWebRequestAndResponse(url, credential);
                return CreateConfigFile(resp, coreSiteFile);
            }
            else
            {
                string url = "http://" + ip + ":" + resourceManagerPort + "/conf";
                if (TryGetResponse(url, out resp))
                {
                    return CreateConfigFile(resp, coreSiteFile);
                }
                else
                {
                    throw (new ConfigFileCreationFailed("\nFailed to create " + coreSiteFile) { UnresolvedUrl = url });
                }
            }
        }

        internal bool IsConfigurationExist()
        {
            if (Directory.Exists(configDirectory))
            {
                FileInfo[] files = (new DirectoryInfo(configDirectory)).GetFiles();
                // Should have Four config files
                if (files.Length >= 4)
                {
                    List<string> fileNames = new List<string>();
                    foreach (FileInfo file in files)
                    {
                        fileNames.Add(file.Name);
                    }
                    if (fileNames.Contains(hdfsSiteFile) && fileNames.Contains(coreSiteFile) && fileNames.Contains(yarnSiteFile) && fileNames.Contains(mapredSiteFile))
                        return true;
                }
            }
            return false;
        }

        public void DeleteConfigurationFolder()
        {
            if (Directory.Exists(configDirectory))
                Directory.Delete(configDirectory, true);
        }

        #endregion

        #region Helper Methods

        private bool TryGetResponse(string url, out string response)
        {
            response = string.Empty;
            try
            {
                WebRequest request = WebRequest.Create(url);
                using (Stream objStream = request.GetResponse().GetResponseStream())
                {
                    if (objStream != null)
                    {
                        using (StreamReader objReader = new StreamReader(objStream))
                        {
                            response = objReader.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;
        }

        public string GetDomainName(string ip, string userName, string password)
        {
            IPHostEntry ipHostEntry = Dns.GetHostEntry(ip);
            string hostName = ipHostEntry.HostName.ToString();
            string domainName = GetADDomainName(userName, password, hostName);
            domainName = domainName.Replace("DC=", ".").Replace(",", string.Empty).TrimStart('.');
            return domainName;
        }
        /// <summary>
        /// Gets node manager ip from RM rest api
        /// </summary>
        /// <param name="url">RM rest api</param>
        /// <returns>Node manager Url</returns>
        private string GetNodeManagerAddress(string response)
        {
            try
            {
                if (!string.IsNullOrEmpty(response))
                {
                    dynamic results = JsonConvert.DeserializeObject(response);
                    JArray nodeArray = results["nodes"]["node"];
                    JToken nodeManagerHost = null;
                    if (nodeArray.Count > 0)
                    {
                        (nodeArray[0] as JObject).TryGetValue("nodeHTTPAddress", out nodeManagerHost);
                    }
                    return nodeManagerHost.ToString();
                }
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// When RM of given IP is stand by, this method checks for active RM and gets NM url
        /// </summary>
        /// <param name="ip">name node ip</param>
        /// <returns>Node manager Url</returns>
        private string TryGetRedirectedNodeManager(WebHeaderCollection headers)
        {
            try
            {
                // Refresh is the key contains redirection links
                if (headers.AllKeys.Contains("Refresh"))
                {
                    string redirectedLink = headers["Refresh"];
                    // Matching regex text between http to the end to get Redirection link
                    MatchCollection mc = Regex.Matches(redirectedLink, "(http)(.*)");
                    if (mc.Count > 0)
                    {
                        // Gets node manager ip from above RM url
                        return GetNodeManagerAddress(mc[0].ToString().TrimEnd(':'));
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        public string GetSecuredWebRequestAndResponse(string sURL, Credential credential)
        {
            HttpWebRequest connectionReq = null;
            var userName = credential.Username;
            var password = credential.Password;
            var domain = credential.DomainName;
            userName = GetUsernameWithDomain(userName, domain);
            try
            {
                if (!string.IsNullOrEmpty(sURL))
                {
                    ServicePointManager.ServerCertificateValidationCallback += ValidateServerCertificate;
                    connectionReq = (HttpWebRequest)WebRequest.Create(sURL);
                    connectionReq.Credentials = new NetworkCredential(userName, password);
                    connectionReq.Timeout = 10000;
                    var httpResponse = (HttpWebResponse)connectionReq.GetResponse();
                    var dataStream = httpResponse.GetResponseStream();
                    var reader = new StreamReader(dataStream);
                    var responseFromServer = reader.ReadToEnd();
                    return responseFromServer;
                }
                return string.Empty;
            }
            catch (WebException wex)
            {
                var Pagecontent = string.Empty;
                var response = (HttpWebResponse)wex.Response;

                if (wex.Response != null)
                {
                    Pagecontent = new StreamReader(wex.Response.GetResponseStream()).ReadToEnd();
                }
                try
                {
                    if (response.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        try
                        {
                            connectionReq = (HttpWebRequest)WebRequest.Create(wex.Response.ResponseUri.AbsoluteUri);
                            connectionReq.Credentials = new NetworkCredential(userName, password, domain);
                            connectionReq.Timeout = 10000;
                            var httpResponse = (HttpWebResponse)connectionReq.GetResponse();
                            var dataStream = httpResponse.GetResponseStream();
                            var reader = new StreamReader(dataStream);
                            var responseFromServer = reader.ReadToEnd();
                            return responseFromServer;
                        }
                        catch (WebException internalWebEx)
                        {
                            if (internalWebEx.Response != null)
                            {
                                Pagecontent = new StreamReader(internalWebEx.Response.GetResponseStream()).ReadToEnd();
                            }
                        }
                    }
                }
                catch (Exception e)
                {

                }
                return "Fail; " + Pagecontent;
            }
            catch (Exception e)
            {
                return "Fail; " + e;
            }
        }

        public WebHeaderCollection GetSecuredWebRequestAndResponseHeader(string sURL, Credential credential)
        {
            HttpWebRequest connectionReq = null;
            var userName = credential.Username;
            var password = credential.Password;
            var domain = credential.DomainName;
            userName = GetUsernameWithDomain(userName, domain);

            try
            {
                if (!string.IsNullOrEmpty(sURL))
                {
                    connectionReq = (HttpWebRequest)WebRequest.Create(sURL);
                    connectionReq.Credentials = new NetworkCredential(userName, password);
                    connectionReq.Timeout = 10000;
                    WebResponse httpResponse = (HttpWebResponse)connectionReq.GetResponse();
                    WebHeaderCollection headers = httpResponse.Headers;
                    return headers;
                }
            }
            catch (Exception e)
            {
            }
            return null;
        }

        /// <summary>
        /// Get Active directory Domain Name.
        /// </summary>
        /// <param name="adUserName">Active Directory Login user</param>
        /// <param name="adPassword">Active Directory Password</param>
        /// <param name="ipAddress">Active Directory ipaddress</param>
        /// <returns></returns>
        private string GetADDomainName(string adUserName, string adPassword, string adHostName)
        {
            try
            {
                string domain = string.Empty;
                string secondPart = "CN=Partitions,CN=Configuration," + adHostName;
                DirectoryEntry root = new DirectoryEntry("LDAP://" + adHostName + "/rootDSE", adUserName, adPassword);
                domain = (string)root.Properties["defaultNamingContext"][0];
                return domain;
            }
            catch (Exception ex)
            {

            }
            return string.Empty;
        }

        private string GetUsernameWithDomain(string username, string domain)
        {
            if (IsUsernameHasDomain(username, domain))
                return username;
            return username + "@" + domain;
        }

        private bool IsUsernameHasDomain(string username, string domain)
        {
            return username.Contains("@") && username.Contains(domain);
        }


        public static bool ValidateServerCertificate(object sender, X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }
        #endregion

        #region Properties

        public string ConfigDirectory
        {
            get
            {
                return configDirectory;
            }
        }

        public string Ip
        {
            get
            {
                return ip;
            }
        }

        public bool IsSecured
        {
            get
            {
                return isSecured;
            }
            set
            {
                isSecured = value;
            }
        }

        public Credential Credential
        {
            get
            {
                return credential;
            }
            set
            {
                credential = value;
            }
        }

        #endregion

        #region Exceptions

        public class ConfigFileCreationFailed : Exception
        {
            public ConfigFileCreationFailed(string message)
                : base(message)
            {
            }
            public string UnresolvedUrl
            {
                get;
                set;
            }
        }

        #endregion

    }
}
