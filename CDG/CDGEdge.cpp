

#include "CDGElement.h"
#include "CDG.h"
#include "DB.h"

void DegTracker::AfterHandleOrder()
{
    deg_changed = false;
    deg_current = deg_actual;
    changed_num_oper = 0;
}
bool DegTracker::BeforeHandleOrder()
{
    deg_changed = false;
    changed_num_oper ++;



    if(changed_num_oper > changed_threshold)
    {

        deg_current = deg_actual;
        changed_num_oper = 0;
        return true;
    }
    return false;
}

void CDGEdge::UpdW()
{
    w_changed = false;
    w_current = w_actual;
}

CDGEdge::CDGEdge(CDGNode* p, const vector<int>& parent_qvs, int next_hop)
{
    src_pt = p;
    parents.emplace_back((CDGElement*)src_pt);
    set<int> ans;
    for(int parent: parent_qvs)
    {
        if(CDG::deg_trackers.count(GenOrderedPair(parent,next_hop)))
        {
            ans.insert(parent);
        }
    }
    join_info = {ans,next_hop};



}
void CDGEdge::CompExactValue()
{
    if(is_exact_value) return;

    dst_pt->CompExactValue();
    LocalUpdate();
    is_exact_value = true;
}
JoinInfo::JoinInfo(const set<int>& p, int qv_)
    {

        qv=qv_;
        for(int parent: p)
        {
            related_deg_trackers.emplace_back(&(CDG::deg_trackers[{parent, qv}]));
            related_deg_values.emplace_back(-1);
        }
    }
void CDGEdge::LocalUpdate()
{
    double cost_val = 0;
    for(auto deg: join_info.related_deg_values)
    {
        cost_val += deg;  
    }
    double num_match = cost_val * w_current;


    if(dst_pt == nullptr)
    {
        value = cost_val;
    }
    else
        value = cost_val + num_match * dst_pt->value;
}



void CDGEdge::UpdStat()
{
    join_info.related_deg_values.clear();
    for(auto* deg_trac: join_info.related_deg_trackers)
    {
        join_info.related_deg_values.emplace_back(deg_trac->deg_actual);
    }
    UpdW();
}

int cdg_edge_update_times = 0;

bool CDGEdge::IncrementalUpdateWithLowerBound(double,CDGElement* child_pt)
{
    visited = false;
    LocalUpdate();
    is_exact_value = child_pt->is_exact_value;
    return true;
}
bool CDGEdge::PropagateLowerBoundTag(CDGElement* child_pt)
{
    is_exact_value = false;
    return true;
}



