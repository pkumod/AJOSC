
#include "CDG.h"
#include "DB.h"
#include <utility>
#include <vector>
#include <queue>
#include <algorithm>
#include <unordered_set>

extern vector<DegTracker*> deg_changed_much;
extern unordered_set<CDGEdge*> w_changed_much;

bool TestChanged(double new_val, double old_val, bool& changed)
{
    if(!changed && abs(old_val - new_val) > 0.4)
    {
        if(new_val == 0 || old_val == 0 || old_val / new_val > 1.5 || new_val / old_val > 1.5)
        {
            need_decide_order = true;
            changed = true;
            return true;
        }
    }
    return false;
}
vector<unsigned char> parents_buffer;

void CDG::UpdateW(double new_weight, const vector<int>& order,int cur_table, int step) {

    parents_buffer.clear();
    parents_buffer.assign(order.begin(), order.begin()+step);
    sort(parents_buffer.begin(),parents_buffer.end());
    parents_buffer.emplace_back(cur_table);

    assert(cdg_edges.count(parents_buffer));
    auto cdg_edge_pt = cdg_edges[parents_buffer];
    auto& w = cdg_edge_pt->w_actual;
    w = w * (1 - delta) + new_weight * delta;
    bool become_changed_much = TestChanged(w, cdg_edge_pt->w_current, cdg_edge_pt->w_changed);
    if(become_changed_much)
    {
        w_changed_much.insert(cdg_edge_pt);
    }
}
void CDG::UpdateDeg(double new_deg, int start_qv, int nbr) {


    auto* deg_trac = &(deg_trackers[{ start_qv, nbr }]);
    bool become_changed_dramatically = deg_trac->UpdateDeg(new_deg);

    if(become_changed_dramatically)
    {
        
        deg_changed_much.emplace_back(deg_trac);
    }
    

}
