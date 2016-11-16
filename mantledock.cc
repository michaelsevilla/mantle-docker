#define _FILE_OFFSET_BITS 64
#include <iostream>
#include <cassert>
#include <dirent.h>
#include <fstream>
#include <vector>
#include <json/value.h>
#include <json/json.h>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/exception/diagnostic_information.hpp> 
#include <boost/thread/thread.hpp>
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.hpp"
#include <curl/curl.h>
#include <chrono>

#define MAXBUFLEN 1000000

using namespace std;
namespace po = boost::program_options;
namespace path = boost::filesystem;

/*
 * Global variables
 */
string docker_skt, container_id, username, container_name;
Json::Value mdss;

/*
 * Om nom some args
 */
void parse_args(int argc, char**argv) 
{
  string path;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce help message")
    ("docker_skt", po::value<string>(&docker_skt)->default_value("/var/run/docker.sock"), "Socket of Docker daemon")
    ("username", po::value<string>(&username)->default_value("root"), "Username for SSHing around")
    ("container_name", po::value<string>(&container_name)->default_value("my_container"), "Name of the container");
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  try { po::notify(vm); }
  catch (exception const& e) {
    cerr << "ERROR: try running with --help\n" << endl;
    cerr << boost::diagnostic_information(e) << endl;
    exit (EXIT_FAILURE);
  }

  if (vm.count("help")) {
    cout << desc << "\n";
    exit (EXIT_SUCCESS);
  }

  cout << "================================" << endl; 
  cout << "  Docker socket = " << docker_skt << endl;
  cout << "  Username = " << username << endl;
  cout << "  Container name = " << container_name << endl;
  cout << "================================" << endl; 
}

/*
 * Get an IP from the metadata stat json
 */
string ip(string mds_gid)
{
  string addr = mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds_gid]["addr"].asString();
  return addr.substr(0, addr.find(":", 0));
}

/*
 * Get a the metadata server name from the stat json
 */
string name(string mds_gid)
{
  return mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds_gid]["name"].asString();
}

/*
 * Check which metadata server owns the spath
 */
string check_subtree(path::path spath) 
{
  /*
   * Connect to ceph
   */
  librados::Rados cluster;
  uint64_t flags;
  assert(!cluster.init2("client.admin", "ceph", flags));
  assert(!cluster.conf_read_file(NULL));
  assert(!cluster.connect());

  /*
   * Get status of metadata cluster
   */
  ceph::bufferlist outbl, inbl;
  string outstring;
  Json::Reader reader;

  /* issue command */
  assert(!cluster.mon_command("{\"prefix\": \"mds stat\", \"format\": \"json\"}", inbl, &outbl, NULL));

  /* parse out mdss */
  reader.parse(outbl.c_str(), mdss);
  cluster.shutdown();

  /*
   * Get the group IDs for active metadata servers
   */
  vector<string> mds_gids;
  assert(mdss["fsmap"]["filesystems"][0]["mdsmap"]["in"].size());
  for (const Json::Value& mds : mdss["fsmap"]["filesystems"][0]["mdsmap"]["in"]) {
    string mds_str = "mds_" + to_string(mds.asInt());
    int gid = mdss["fsmap"]["filesystems"][0]["mdsmap"]["up"][mds_str].asInt();
    mds_gids.push_back("gid_" + to_string(gid));
  }

  /*
   * Get the subtrees for each metadata server and store in map (mds_gid |-> tree)
   */
  vector<pair<string, Json::Value>> trees;
  for (auto& mds : mds_gids) {
    string name = mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds]["name"].asString();

    /* construct command to send to servers admin daemon socket */
    string cmd = "ssh " + username + "@" + ip(mds) + " \
                 \"docker exec ceph-dev /bin/bash -c \
                 \'cd /ceph/build && bin/ceph daemon mds." + name + " get subtrees\'\" 2>/dev/null";
 
    /* issue command */
    char buff[MAXBUFLEN];
    Json::Value tree;
    FILE *f = popen(cmd.c_str(), "r");
    fread(buff, 1, MAXBUFLEN, f);

    /* parse out subtrees */
    reader.parse(buff, tree);
    trees.push_back(pair<string, Json::Value>(mds, tree));
  }

  /*
   * Figure out who owns the subtree
   * - if there is an exact match to the path, save the metadata server in auth
   * - otherwise take the longest path, which is the subtree for our search path
   */
  string auth = "";
  string mds0 = mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds_gids.at(0)]["name"].asString();
  pair<string, path::path> max_path = make_pair(mds0, path::path(""));

  /* search for spath AND save state for finding max path length */
  for (auto& tree : trees) {
    int n = tree.second.size();
    for (int i = 0; i < n; i++) {
      path::path p = path::path(tree.second[i]["dir"]["path"].asString());
      bool is_auth = tree.second[i]["dir"]["is_auth"].asBool();

      /* if the metadata server has our full path, save it */
      if (is_auth && spath == p)
        auth = tree.first;

      /* save path if it is the largest (in case we don't find exact match) */
      if (is_auth &&
          p.size() > max_path.second.size() &&
          equal(p.begin(), p.end(), spath.begin())) {
        max_path = make_pair(tree.first, p); 
      }
    }
  }

  /* if we didn't find an exact match, choose the next best thing */
  if (auth == "")
    auth = max_path.first;

  return auth;
}

/*
 * Tie a container to an inode
 */
path::path create_container_inode()
{
  /* 
  * Connect to CephFS
  */
  struct ceph_mount_info *cmount;
  assert(!ceph_create(&cmount, "admin"));
  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_env(cmount, NULL);
  assert(!ceph_mount(cmount, "/"));
  
  /*
   * Create some fake data
   */
  string s = "/containers/" + container_name;
  ceph_mkdir(cmount, "containers", 0644);
  ceph_mkdir(cmount, s.c_str(), 0644);
   
  /*
   * Cleanup 'errbody
   */
  ceph_unmount(cmount);
  ceph_release(cmount);

  return path::path(s);
}

/*
 * Ugly curl output parsing
 * https://techoverflow.net/blog/2013/03/15/c-simple-http-download-using-libcurl-easy-api/
 */ 
size_t write_data(void *ptr, size_t size, size_t nmemb, void *stream) {
  string data((const char*) ptr, (size_t) size * nmemb);
  *((stringstream*) stream) << data << endl;
  return size * nmemb;
}

/*
 * Get list of running containers
 */
void docker_ps(void *curl, string docker_url)
{
  /* Point at Docker */
  string target = docker_url + "/containers/json";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
  curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, docker_skt.c_str());

  /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: " << curl_easy_strerror(ret) << endl;

  /* Get it into JSON */
  Json::Value docker_ps;
  Json::Reader reader;
  reader.parse(out.str(), docker_ps);

  cout << "-- Running containers:" << endl;
  for (auto& container : docker_ps)
    for (auto& name : container["Names"])
      cout << "  " << name.asString() << endl;
  curl_easy_reset(curl);
}

/*
 * Start a container on the remote node
 */
void docker_create(void *curl, string docker_url)
{
   /* Point at Docker */
  string target = docker_url + "/containers/create?name=" + container_name;
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
  curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, docker_skt.c_str());

  /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

  /* Set up JSON header */
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Accept: application/json");
  headers = curl_slist_append(headers, "Content-Type: application/json");
  string args = "{ \
                   \"Image\":\"tutum/ubuntu:trusty\", \
                   \"Detach\":true, \
                   \"Tty\":true, \
                   \"Entrypoint\":\"/bin/bash\" \
                 }";
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, args.c_str());

  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: " << curl_easy_strerror(ret) << endl;

  /* Get it into JSON */
  Json::Value docker_start;
  Json::Reader reader;
  reader.parse(out.str(), docker_start);
  curl_easy_reset(curl);

  container_id = docker_start["Id"].asString();
  if (container_id == "") {
    cerr << "ERROR: " << out.str() << endl;  
    exit(EXIT_FAILURE);
  }
  cout << "-- Creating containers:" << container_id << endl;
}

void docker_start(void* curl, string docker_url) {
   /* Point at Docker */
  cout << "-- Starting containers:" << container_id << endl;
  string target = docker_url + "/containers/" + container_id + "/start";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
  curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, docker_skt.c_str());

   /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
 
  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: " << curl_easy_strerror(ret) << endl;
  curl_easy_reset(curl);
}

void docker_rm(void *curl, string docker_url)
{
  /* Point at Docker */
  string target = docker_url + "/containers/" + container_id + "?force=true";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
  curl_easy_setopt(curl, CURLOPT_UNIX_SOCKET_PATH, docker_skt.c_str());

   /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
 
  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: " << curl_easy_strerror(ret) << endl;
  cout << "-- Removed containers:" << endl;
  cout << "  " << out.str() << "\n" << endl;
  curl_easy_reset(curl);
} 

int main(int argc, char **argv)
{
  parse_args(argc, argv);

  /* Set up curl */
  curl_global_init(CURL_GLOBAL_DEFAULT);
  void *curl = curl_easy_init();  

  /* Tie container to inode */
  path::path spath = create_container_inode();
  string mds_gid = check_subtree(spath); 
  cout << "... container tied to spath=" << spath 
       << " on mds." << name(mds_gid) << endl;

  /* Start container on target metadata server */
  docker_create(curl, ip(mds_gid));
  docker_start(curl, ip(mds_gid));
  docker_ps(curl, ip(mds_gid));

  while (1) {
    string new_mds_gid = check_subtree(spath);
    cout << "INFO: container " << container_name << " with path " << spath << " is on mds." << name(new_mds_gid) << endl;;
    if (new_mds_gid != mds_gid) {
      cout << " INFO: inode has moved from mds." << name(mds_gid)
           << " to mds." << name(new_mds_gid) << "." << endl;
      docker_rm(curl, ip(mds_gid));
      docker_create(curl, ip(new_mds_gid));
      docker_start(curl, ip(new_mds_gid));
      docker_ps(curl, ip(new_mds_gid));
      mds_gid = new_mds_gid;
    }
    boost::this_thread::sleep( boost::posix_time::seconds(1));
  }

  /* This leaves "still reachable" blocks; too lazy to fix */
  curl_easy_cleanup(curl);
  curl_global_cleanup();
  return 0;
}
