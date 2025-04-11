
#include "CDG.h"
#include "DB.h"
#include <utility>
#include <vector>
#include <queue>
#include <algorithm>
#include <chrono>
#include <unordered_set>

vector<DegTracker *> deg_changed_much;
unordered_set<CDGEdge *> w_changed_much;
int decide_order_freq = 0;



void CDG::IncrDecideOrder()
{ 
    
    for (auto cdg_edge : w_changed_much)
    {
        
        cdg_edge->UpdStat();
        double old_val = cdg_edge->value;
        cdg_edge->LocalUpdate();
        double new_val = cdg_edge->value;

        LowerBoundUpdate(cdg_edge, new_val, old_val);
        
    }
    


    for (auto cdg_node : layered_cdg_nodes.back())
    {
        cdg_node->CompExactValue();
    }


    w_changed_much.clear();

    int qv_id = 0;
    for (auto &order : orders)
    {
        order.clear();
        order.emplace_back(qv_id);
        auto cdg_node = roots[qv_id];
        while (cdg_node != nullptr)
        {
            order.emplace_back(cdg_node->opt_child->join_info.qv);
            cdg_node = cdg_node->opt_child->dst_pt;
        }
        qv_id++;
    }
}

vector<int> CDG::GetMatchingOrder(int start_qv)
{
    return orders[start_qv];
}

/**
 * @return whether we need to recompute the order.
 */
bool CDG::MonitorStat()
{


    for (auto deg_trac : deg_changed_much)
    {

        bool need_handle_order = deg_trac->BeforeHandleOrder();
        if (need_handle_order)
        {

            for (auto cdg_edge : deg_trac->related_cdg_edges)
            {
                w_changed_much.insert(cdg_edge);
            }
        }
    }
    deg_changed_much.clear();
    need_decide_order = false;
    

    return !w_changed_much.empty();
}

void CDG::DecideOrder()
{
    Update();

    int qv_id = 0;
    for (auto &order : orders)
    {
        order.clear();
        order.emplace_back(qv_id);
        auto cdg_node = roots[qv_id];
        while (cdg_node != nullptr)
        {
            order.emplace_back(cdg_node->opt_child->join_info.qv);
            cdg_node = cdg_node->opt_child->dst_pt;
        }
        qv_id++;
    }
    
    need_decide_order = false;
}