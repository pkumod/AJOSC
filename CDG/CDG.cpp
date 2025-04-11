#include "CDG.h"
#include "DB.h"
#include <utility>
#include <vector>
#include <queue>
#include <algorithm>



unordered_map<pair<int,int>, DegTracker, boost::hash<pair<int,int>>> CDG::deg_trackers = {};



void CDG::SetChildPtrs()
{
    int layer_idx = 0;
    for(auto& layer: layered_cdg_edges)
    {
        for(auto cdg_edge_pt: layer)
        {
            cdg_edge_pt->src_pt->children.emplace_back(cdg_edge_pt);
            cdg_edge_pt->src_pt->opt_child = cdg_edge_pt;
            assert(cdg_edge_pt);
        }
        for(auto cdg_node_pt: layered_cdg_nodes[layer_idx])
        {
            for(auto parent_edge: cdg_node_pt->parents)
            {
                ((CDGEdge*)parent_edge)->dst_pt = cdg_node_pt;
            }
        }

        layer_idx++;
    }
}

int num_cdg_edges =  0;
int num_cdg_nodes =  0;

vector<unsigned char> GetUCharVec(const vector<int>& vec, int nbr)
{
    vector<unsigned char> parents;
    parents.assign(vec.begin(),vec.end());
    std::sort(parents.begin(), parents.end());
    parents.emplace_back(nbr);
    return parents;
}
void CDG::Init(const Query &q,DB* db_) {

    struct MatchedInfo
    {
        set<int> qvs;
        MatchedInfo(const vector<int>& vec)
        {
            for(int qv:vec)
            {
                qvs.insert(qv);
            }
        }
        MatchedInfo(const set<int>& s):qvs(s)
        {}
        bool operator<(const MatchedInfo & j) const
        {
            if (qvs< j.qvs)return true;
            return false;
        }
       
    };

    map<MatchedInfo,CDGNode*> cdg_nodes;  
    orders.resize(DB::num_qvs);
    db = db_;
    
    int qv_id = 0;
    for(auto& nbr_list: q.adj_list)
    {
        for(int nbr: nbr_list)
        {
            deg_trackers.insert({{qv_id,nbr},{}});
        }
        qv_id ++;
    }


    
    layered_cdg_nodes.resize(DB::num_qvs-1);
    layered_cdg_edges.resize(DB::num_qvs-1);

    struct PartialQ
    {
        vector<int> qvs;
        set<int> connected_qvs;  /// the qvs that connect to qvs
        vector<int> parent_qvs;
        int added_qv = -1;
    public:
        void Add(int qv, const Query& q_)
        {
            parent_qvs = qvs;
            added_qv = qv;
            qvs.emplace_back(qv);
            sort(qvs.begin(), qvs.end());
            connected_qvs.erase(qv);

            for(int nbr: q_.adj_list[qv])
            {
                if(!count(qvs.begin(),qvs.end(),nbr)) connected_qvs.insert(nbr);
            }

        }
        bool operator<(const PartialQ& pq) const
        {
            return qvs < pq.qvs;
        }
    };
    queue<PartialQ> partial_queries;

    set<vector<int>> visited;
    roots.resize(DB::num_qvs, nullptr);

    for(qv_id = 0; qv_id < DB::num_qvs; qv_id++)
    {
        PartialQ start_pq; start_pq.Add(qv_id,q);
        partial_queries.push(start_pq);
    }

    while (!partial_queries.empty())
    {
        auto pq = partial_queries.front();
        partial_queries.pop();
        if(visited.count(pq.qvs)){
            
            auto parents = GetUCharVec(pq.parent_qvs, pq.added_qv);
            
            cdg_nodes.at(pq.qvs)->parents.emplace_back(cdg_edges[parents]);
            continue;
        }
        visited.insert(pq.qvs);

        auto cdg_node_ptr = new CDGNode(pq.connected_qvs);
        if(!pq.parent_qvs.empty()) {
            auto parents = GetUCharVec(pq.parent_qvs, pq.added_qv);
            cdg_node_ptr->parents.emplace_back(cdg_edges[parents]);
        }/// CDGNode.parents is not fully initialized here. it is gradually initialized in "if(visited.count(pq))".
        cdg_nodes.insert({pq.qvs, cdg_node_ptr});
        if(pq.qvs.size() == 1)
        {
            roots[pq.qvs[0]] = cdg_node_ptr;
        }
        if(DB::num_qvs != pq.qvs.size())
        {
            layered_cdg_nodes[DB::num_qvs-pq.qvs.size()-1].emplace_back(cdg_node_ptr);
            num_cdg_nodes++;
        }

        for(auto nbr: pq.connected_qvs)
        {
            auto cdg_edge_pt = new CDGEdge(cdg_node_ptr,pq.qvs,nbr);
            for(auto parent: pq.qvs)
            {
                if(deg_trackers.count({parent, nbr}))
                {
                    deg_trackers[{parent, nbr}].related_cdg_edges.emplace_back(cdg_edge_pt);
                }
            }
            auto parents = GetUCharVec(pq.qvs, nbr);
            cdg_edges.insert({parents,cdg_edge_pt});
            num_cdg_edges++;
//            join_info_2_cdg_edges.insert({cdg_edge_pt->join_info, cdg_edge_pt});
            layered_cdg_edges[DB::num_qvs-pq.qvs.size()-1].emplace_back(cdg_edge_pt);

            auto extended_pq = pq;
            extended_pq.Add(nbr,q);
            partial_queries.push(extended_pq);
        }
    }

    SetChildPtrs();
//    for(auto& [join_info, pt]: join_info_2_cdg_edges)
//    {
//        for(int t: join_info.parents)
//            cout<<t<<' ';
//        cout<<join_info.table<<' '<<pt<<endl;
//    }

//exit(0);
}

