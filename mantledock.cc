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
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.hpp"

#define MAXBUFLEN 1000000

using namespace std;
namespace po = boost::program_options;
namespace path = boost::filesystem;

/*
 * Global variables
 */
path::path spath; // search path... find the metadata server for this path!

/*
 * Om nom some args
 */
void parse_args(int argc, char**argv) 
{
  string path;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "Produce help message")
    ("path",  po::value<string>(&path)->required(), "Path to match to MDS");
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

  if (path[0] == '/')
    path.erase(0, 1);
  spath = path::path(path); 
  cout << "Searching for path=" << spath << endl;;
}

/*
 * Check which metadata server owns the spath
 */
string check_subtree() 
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
  Json::Value mdss;
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
   * Get the subtrees for each metadata server (not sure why only CLI works)
   */
  vector<pair<string, Json::Value>> trees;
  for (auto& mds : mds_gids) {
    string m = mdss["fsmap"]["filesystems"][0]["mdsmap"]["info"][mds]["name"].asString();
    const char *cmd = ("ceph daemon mds." + m + " get subtrees").c_str();
    char buff[MAXBUFLEN];
    Json::Value tree;
 
    /* issue command */
    FILE *f = popen(cmd, "r");
    fread(buff, 1, MAXBUFLEN, f);

    /* parse out subtrees */
    reader.parse(buff, tree);
    trees.push_back(pair<string, Json::Value>(m, tree));
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

      /* if the metadata server has our full path, save it */
      if (spath == p)
        auth = tree.first;

      /* save path if it is the largest (in case we don't find exact match) */
      if (p.size() > max_path.second.size() &&
          equal(p.begin(), p.end(), spath.begin())) {
        max_path = make_pair(tree.first, p); 
      }
    }
  }

  /* if we didn't find an exact match, choose the next best thing */
  if (auth == "")
    auth = max_path.first;
  cout << "auth=" << auth << endl;
  return auth;
}

/*
 * Tie a container to an inode
 */
void create_container_inode()
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
  ceph_mkdir(cmount, "blah", 0644);
   
  /*
   * Cleanup 'errbody
   */
  ceph_unmount(cmount);
  ceph_release(cmount);
}

int main(int argc, char **argv)
{
  parse_args(argc, argv);
  string target = check_subtree();
  create_container_inode();
  return 0;
}
