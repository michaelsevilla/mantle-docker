#define _FILE_OFFSET_BITS 64
#include <iostream>
#include <cassert>
#include <dirent.h>
#include <signal.h>
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
#include <unistd.h>

#define MAXBUFLEN 1000000
#define VERSION "1.0.0"

using namespace std;
namespace po = boost::program_options;
namespace path = boost::filesystem;
void * curl;

/*
 * Global variables
 */
string docker_skt, container_id, username, container_name, container_conf;
char mode;
Json::Value mdss;
int debug;
bool vstart;

/*
 * Om nom some args
 */
void parse_args(int argc, char**argv) 
{
  string path;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce help message and version")
    ("docker_skt", po::value<string>(&docker_skt)->default_value("/var/run/docker.sock"), "Socket of Docker daemon")
    ("username", po::value<string>(&username)->default_value("root"), "Username for SSHing around")
    ("container_name", po::value<string>(&container_name)->default_value("my_container"), "Name of the container")
    ("mode", po::value<char>(&mode)->default_value('w'), "Load mode, can be r or w for read or write, respectively")
    ("debug", po::value<int>(&debug)->default_value(0), "Turn on debugging")
    ("vstart", po::value<bool>(&vstart)->default_value(false), "Use vstart")
    ("container_conf", po::value<string>(&container_conf)->required(), "JSON file with args for container");
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  try { po::notify(vm); }
  catch (exception const& e) {
    cerr << boost::diagnostic_information(e) << endl;
    cout << "Mantle Docker version " << VERSION
         << "\n" << desc << "\n";
    exit (EXIT_FAILURE);
  }

  if (vm.count("help")) {
    cout << "Mantle Docker version " << VERSION
         << "\n" << desc << "\n";
    exit (EXIT_SUCCESS);
  }

  cout << "================================" << endl; 
  cout << "  Docker socket = " << docker_skt << endl;
  cout << "  Username = " << username << endl;
  cout << "  Container name = " << container_name << endl;
  cout << "  Container conf = " << container_conf << endl;
  cout << "  Metadata Load Mode = " << mode << endl;
  cout << "  Debug = " << debug << endl;
  cout << "  vstart = " << vstart << endl;
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
 * Get the metadata server name from the stat json
 */
string name(string mds_gid)
{
  return mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds_gid]["name"].asString();
}

/*
 * Get status of metadata cluster
 */
void mds_stats()
{
  /* connect to Ceph */
  librados::Rados cluster;
  uint64_t flags;
  assert(!cluster.init2("client.admin", "ceph", flags));
  assert(!cluster.conf_read_file(NULL));
  assert(!cluster.connect());

  /* issue command */
  ceph::bufferlist outbl, inbl;
  string outstring;
  Json::Reader reader;
  assert(!cluster.mon_command("{\"prefix\": \"mds stat\", \"format\": \"json\"}", inbl, &outbl, NULL));
  reader.parse(outbl.c_str(), mdss);

  /* cleanup Ceph connection */
  cluster.shutdown();
}

/*
 * Figure out which metadata server owns the given container inode (cinode)
 * - returns the metadata server gid
 */
string mds_subtree(path::path cinode) 
{
  if (debug > 1)
    cout << "INFO: mds_subtree: got mdss" << mdss << endl;

  /*
   * Get the group IDs for active metadata servers
   */
  vector<string> mds_gids;
  assert(mdss["fsmap"]["filesystems"][0]["mdsmap"]["in"].size());
  for (const Json::Value& mds : mdss["fsmap"]["filesystems"][0]["mdsmap"]["in"]) {
    string mds_str = "mds_" + to_string(mds.asInt());
    int gid = mdss["fsmap"]["filesystems"][0]["mdsmap"]["up"][mds_str].asInt();
    mds_gids.push_back("gid_" + to_string(gid));
    if (debug > 1)
      cout << "INFO: mds_subtree: got mds_gids=gid_" << to_string(gid) << endl;
  }

  /*
   * Get the subtrees for each metadata server and store in map (mds_gid |-> tree)
   */
  vector<pair<string, Json::Value>> trees;
  for (auto& mds : mds_gids) {
    string name = mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds]["name"].asString();

    /* construct command to send to servers admin daemon socket */
    string cmd = "ssh " + username + "@" + ip(mds) + " \"docker exec ";
    if (vstart == true)
      cmd += "ceph-dev /bin/bash -c \'cd /ceph/build && bin/";
    else
      cmd += "ceph-" + name + "-mds /bin/bash -c \'";
    cmd += "ceph daemon mds." + name + " get subtrees\'\" 2>/dev/null";
    if (debug > 1)
      cout << "INFO: mds_subtree: cmd=" << cmd << endl;
 
    /* issue command */
    char buff[MAXBUFLEN];
    Json::Value tree;
    FILE *f = popen(cmd.c_str(), "r");
    fread(buff, 1, MAXBUFLEN, f);

    /* parse out subtrees */
    Json::Reader reader;
    reader.parse(buff, tree);
    trees.push_back(pair<string, Json::Value>(mds, tree));
    if (debug > 1)
      cout << "INFO: mds_subtree: name=" << name << " gid=" << mds << " tree=" << tree << endl;
  }

  /*
   * Figure out who owns the subtree
   * - if there is an exact match to the path, save the metadata server in auth
   * - otherwise take the longest path, which is the subtree for our search path
   */
  string auth = "";
  pair<string, path::path> max_path = make_pair(mds_gids.at(0), path::path(""));

  /* search for cinode AND save state for finding max path length */
  for (auto& tree : trees) {
    int n = tree.second.size();
    for (int i = 0; i < n; i++) {
      path::path p = path::path(tree.second[i]["dir"]["path"].asString());
      bool is_auth = tree.second[i]["dir"]["is_auth"].asBool();
      if (debug > 0)
        cout << "  INFO: mds_subtree: checking if"
             << " p=" << p << " is a parent or equal to cinode=" << cinode
             << " max_path.gid=" << max_path.first << " max_path.p=" << max_path.second 
             << " auth=" << auth << endl;

      /* if the metadata server has our full path, save it */
      if (is_auth && cinode == p) {
        if (debug > 0)
          cout << "  INFO: mds_subtree: found exact match;"
               << " is_auth=" << is_auth
               << " cinode=" << cinode << " p=" << p << endl;
        auth = tree.first;
      }

      /* save path if it is the largest (in case we don't find exact match) */
      if (is_auth &&
          p.size() > max_path.second.size() &&
          equal(p.begin(), p.end(), cinode.begin())) {
        if (debug > 0)
          cout << "  INFO: mds_subtree: found new max;"
               << " p=" << p 
               << " prev=" << max_path.second 
               << " p is subpath of cinode=" << cinode
               << endl;
        max_path = make_pair(tree.first, p); 
      }
    }
  }

  /* if we didn't find an exact match, choose the next best thing */
  if (auth == "")
    auth = max_path.first;

  if (auth == "") {
    cerr << "ERROR: could not get subtree authority for cinode=" << cinode << endl;
    exit(EXIT_FAILURE);
  }

  return auth;
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
void docker_ps(string docker_url)
{
  /* Point at Docker */
  string target = docker_url + ":2375/containers/json";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());

  /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: docker ps: " << curl_easy_strerror(ret) 
         << "\n\t target=" << target << endl;

  /* Get it into JSON */
  Json::Value docker_ps;
  Json::Reader reader;
  reader.parse(out.str(), docker_ps);

  cout << "-- Running containers: target=" << target << endl;
  for (auto& container : docker_ps)
    for (auto& name : container["Names"])
      cout << "  " << name.asString() << endl;
  curl_easy_reset(curl);
}

/*
 * Start a container on the remote node
 */
void docker_create(string docker_url)
{
   /* Point at Docker */
  string target = docker_url + ":2375/containers/create?name=" + container_name;
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());

  /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

  /* Set up JSON header */
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Accept: application/json");
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  ifstream t(container_conf);
  string args((istreambuf_iterator<char>(t)),
               istreambuf_iterator<char>());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, args.c_str());

  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: creating container: " << curl_easy_strerror(ret)
         << "\n\t target=" << target << endl;

  /* Get it into JSON */
  Json::Value docker_start;
  Json::Reader reader;
  reader.parse(out.str(), docker_start);
  curl_easy_reset(curl);

  container_id = docker_start["Id"].asString();
  if (container_id == "") {
    cerr << "ERROR creating container: " << out.str()
         << "\n\t target=" << target << endl;
    exit(EXIT_FAILURE);
  }
  cout << "-- Creating containers:" << container_id << endl;
}

void docker_start(string docker_url)
{
   /* Point at Docker */
  string target = docker_url + ":2375/containers/" + container_id + "/start";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
  cout << "-- Starting containers:" << container_id << " target=" << target << endl;

   /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
 
  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: docker start: " << curl_easy_strerror(ret)
         << "\n\t target=" << target << endl;
  curl_easy_reset(curl);
}

void docker_rm(string docker_url)
{
  /* Point at Docker */
  string target = docker_url + ":2375/containers/" + container_id + "?force=true";
  curl_easy_setopt(curl, CURLOPT_URL, target.c_str());

   /* Stuff output in string */
  std::stringstream out;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
  curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
 
  /* Execute command */
  CURLcode ret = curl_easy_perform(curl);
  if (ret != CURLE_OK)
    cerr << "ERROR: docker rm: " << curl_easy_strerror(ret)
         << "\n\t target=" << target << endl;
  cout << "-- Removed containers: target=" << target << endl;
  cout << "  " << out.str() << "\n" << endl;
  curl_easy_reset(curl);
} 

volatile sig_atomic_t flag = 0;
void interrupt(int sig)
{
  if (sig == SIGINT) {
    flag = 1;
    cout << "... OUCH!" << endl;
  }
}

/*
 * Make metadata load until we receive an interrupt
 */
void create_metadata_load()
{
  /* connect to CephFS */
  struct ceph_mount_info *cmount;
  assert(!ceph_create(&cmount, "admin"));
  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_env(cmount, NULL);
  assert(!ceph_mount(cmount, "/"));

  /* create metadata load */
  string file = "/containers/" + container_name + "/cinode.txt";
  switch (mode) {
    case 'w':
      while(!flag) {
        int fd = ceph_open(cmount, file.c_str(), O_CREAT|O_RDWR, 0644);
        ceph_close(cmount, fd);
        assert(!ceph_unlink(cmount, file.c_str()));
      }
      break;
    case 'r':
      int fd = ceph_open(cmount, file.c_str(), O_CREAT|O_RDWR, 0644);
      assert(fd >= 0);
      while (!flag)
        ceph_lseek(cmount, fd, 0, SEEK_END);
      break;
  }

  cout << "INFO: cleaning up metadata load CephFS connection" << endl;
  assert(!ceph_unmount(cmount));
  assert(!ceph_release(cmount));
}

/*
 * Monitor the container inode and migrate container if it moves
 */
void docker_monitor(path::path cinode)
{
  /* set up curl */
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();  

  /* start monitoring */
  string mds_gid = mds_subtree(cinode);
  while(!flag) {
    string new_mds_gid = mds_subtree(cinode);
    cout << "INFO: docker_monitor: container " << container_name << " with path "
         << cinode << " is on mds." << name(new_mds_gid) << endl;
    if (new_mds_gid != mds_gid) {
      cout << "INFO: docker_monitor: inode has moved from mds." << name(mds_gid)
           << " to mds." << name(new_mds_gid) << "." << endl;
      docker_rm(ip(mds_gid));
      docker_create(ip(new_mds_gid));
      docker_start(ip(new_mds_gid));
      docker_ps(ip(new_mds_gid));
      mds_gid = new_mds_gid;
    }
    boost::this_thread::sleep( boost::posix_time::seconds(1));
  }

  cout << "INFO: docker_monitor: cleaning cURL" << endl;
  curl_easy_cleanup(curl);
  curl_global_cleanup();
}

/*
 * Tie a container to an inode
 */
path::path create_container_inode()
{
  /* connect to CephFS */
  struct ceph_mount_info *cmount;
  assert(!ceph_create(&cmount, "admin"));
  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_env(cmount, NULL);
  assert(!ceph_mount(cmount, "/"));

  /* create the container inode */
  string dir = "/containers/" + container_name;
  ceph_mkdirs(cmount, dir.c_str(), 0644);
  string file = "/containers/" + container_name + "/cinode.txt";
  path::path cinode = path::path(dir);

  /* tie container to inode */
  string mds_gid = mds_subtree(cinode); 
  cout << "... container tied to cinode=" << cinode 
       << " on mds." << name(mds_gid) << " ip=" << ip(mds_gid) << endl;

  /* start container on target metadata server */
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();  
  docker_create(ip(mds_gid));
  docker_start(ip(mds_gid));
  docker_ps(ip(mds_gid));

  /* cleanup 'errbody */
  curl_easy_cleanup(curl);
  curl_global_cleanup();
  assert(!ceph_unmount(cmount));
  assert(!ceph_release(cmount));

  cout << "INFO: created cinode=" << cinode << endl;
  return path::path(cinode);
}

int main(int argc, char **argv)
{
  parse_args(argc, argv);
  mds_stats();
  path::path cinode = create_container_inode();

  signal(SIGINT, interrupt);
  pid_t pid = fork();
  if (pid)
    create_metadata_load();
  else if (!pid) 
    docker_monitor(cinode);
  cout << "INFO: EXITING" << endl;
  return 0;
}
