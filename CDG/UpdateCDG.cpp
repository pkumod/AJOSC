

#include "CDG.h"
#include "DB.h"
#include <utility>
#include <vector>
#include <queue>
#include <algorithm>

/**
 * @brief update values in the CDG, given cost/drop_vals which are collected during executing the query.
 */
void CDG::Update()
{
    

    for(auto& [_,deg_trac]: deg_trackers)
    {
        deg_trac.AfterHandleOrder();
    }

    int layer_idx = 0;
    for(auto& layer: layered_cdg_edges)
    {
        for(auto cdg_edge: layer)
        {
            cdg_edge->UpdStat();
            cdg_edge->LocalUpdate();
        }
        for(auto cdg_node_pt: layered_cdg_nodes[layer_idx])
        {
            cdg_node_pt->LocalUpdate();
        }

        layer_idx++;
    }



}
int updated_elements_num = 0;
int propagate_tag_num = 0;

queue< tuple<CDGElement*, CDGElement*> > elements_to_propagate;  /// 2nd CDGElement*: child pt. if parent value = child value, then "is exact value = false"  need to propagate. else the propagation ends.

/**
 * @note before execute this function, cdg_edge has changed its parent's next_qv_2_choose_val's corresponding place.
 * @param cdg_edge
 * @param new_val
 * @param old_val
 */
void CDG::LowerBoundUpdate(CDGEdge* cdg_edge, double new_val, double old_val)
{
    if(new_val == old_val) return;
    if(new_val > old_val)
    {
        if(!cdg_edge->src_pt->is_exact_value) return;  /// no need to propagate. tag is already set.

        /// if it meets best order, we cannot endure it to be lower bound.
        
        elements_to_propagate.emplace(cdg_edge->src_pt, cdg_edge);

        while(!elements_to_propagate.empty())
        {
            auto [element, child_pt] = elements_to_propagate.front();
            elements_to_propagate.pop();

            bool need_propagate = element->PropagateLowerBoundTag(child_pt);
            propagate_tag_num++;
            if(need_propagate)
            {
                
            

                for(auto parent: element->parents)
                {
                    assert(parent != nullptr);
                    if(parent->is_exact_value){
                        elements_to_propagate.emplace(parent,element);
                    }
                }

            }
        }

    }

    else  /// new value is smaller. we may need to propagate it.
    {
//        cout<<"value decrease\n";
        queue< tuple<CDGElement*,double,CDGElement*> > elements_to_update;  /// double: new_value. 2nd CDGElement*: child pt
        elements_to_update.emplace(cdg_edge->src_pt, new_val, cdg_edge);

        while(!elements_to_update.empty())
        {
            auto [element,child_new_val,child_pt] = elements_to_update.front();
            elements_to_update.pop();

            bool need_propagate = element->IncrementalUpdateWithLowerBound(child_new_val ,child_pt);
            updated_elements_num++;
            auto new_value = element->value;

            if(need_propagate)
            {
                
                
                for(auto parent: element->parents)
                {
                    assert(parent != nullptr);
                    if(!parent->visited){
                        parent->visited = true;
                        elements_to_update.emplace(parent,new_value ,element);
                    }
                    else
                    {
                        parent->visited_more_than_once = true;
                    }
                }

            }
        }
    }

}

