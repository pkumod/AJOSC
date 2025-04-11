#ifndef CONJUNCTIVEQ_STATISTICS_H
#define CONJUNCTIVEQ_STATISTICS_H
#include "Query.h"
#include <map>
#include <utility>
#include <vector>
#include "hopscotch_map.h"
#include "hopscotch_set.h"
#include <set>
#include "CDGElement.h"
#include <boost/functional/hash.hpp>
#include "unordered_map"


using namespace std;
class DB;


class CDG
{
public:
    
    vector<vector<int>> orders;
    tsl::hopscotch_map<vector<unsigned char>, CDGEdge*,boost::hash<vector<unsigned char>>> cdg_edges;
 
    vector<vector<CDGEdge*>> layered_cdg_edges;  
    vector<vector<CDGNode*>> layered_cdg_nodes;

    static unordered_map<pair<int,int>, DegTracker, boost::hash<pair<int,int>>> deg_trackers; 

    vector<CDGNode*> roots;
    DB* db;

    void Update();
    void LowerBoundUpdate(CDGEdge* cn, double, double);
   
public:
    vector<int> GetMatchingOrder(int start_qv);

    bool MonitorStat();

    void DecideOrder();

    void Init(const Query& q, DB* db_);

    void UpdateW(double w, const vector<int>& order,int cur_qv, int step);
    void UpdateDeg(double , int start_qv, int nbr);



    void IncrDecideOrder();

    void SetChildPtrs();

};

#endif