

#include "CDGElement.h"
#include "CDG.h"





CDGNode::CDGNode(const set<int>& valid_next_hops_)
{
}
void CDGNode::LocalUpdate()
{
    value = inf;
    for(auto* child_pt: children)
    {
        if(child_pt->value < value)
        {
            value = child_pt->value;
            opt_child = child_pt;
        }
    }
    assert(opt_child);

}
int cdgnodeUpdateTimes = 0;

bool CDGNode::IncrementalUpdateWithLowerBound(double new_choose_val ,CDGElement* child_pt)
{
    visited = false;
    if(visited_more_than_once)
    {
        visited_more_than_once = false;
        auto origin_min_val = value;
        auto origin_opt_child = opt_child;
        value = inf;
        opt_child = nullptr;
        for(auto child_pt: children)
        {
            if(child_pt->value < value)
            {
                value = child_pt->value;
                opt_child = child_pt;
            }
        }
        if(opt_child == nullptr)opt_child = children[0];
        if(value < origin_min_val)
        {
            is_exact_value = opt_child->is_exact_value;
            return true;
        }
        else
        {
            value = origin_min_val;
            opt_child = origin_opt_child;
            assert(opt_child);
            
            return false;
        }


    }
    assert(visited_more_than_once == false);

    if(new_choose_val < value)
    {
        value = new_choose_val;
        opt_child = (CDGEdge*)child_pt;
        assert(child_pt);
        is_exact_value = opt_child->is_exact_value;
        return true;
    }


    return false;
}

void CDGNode::CompExactValue()
{
    if(is_exact_value)
    {
        return;
    }

    child_val2id.clear();

    for(auto* child_pt: children)
    {
        child_val2id.emplace_back(child_pt->value, child_pt);
    }
    sort(child_val2id.begin(), child_val2id.end());

    value = inf;
    opt_child = nullptr;

    for(int i = 0; i < child_val2id.size(); i++)
    {
        auto [child_val, child_pt] = child_val2id[i];
        child_pt->CompExactValue();
        if(value > child_pt->value)
        {
            value = child_pt->value;
            opt_child = child_pt;
        }
        if( i+1 == child_val2id.size() || value <= child_val2id[i + 1].first)
        {
            is_exact_value = true;
            return;
        }
    }
    if(opt_child == nullptr)opt_child = children[0];
}

/**
         *
         * @param old_child_value
         * @return whether we need to propagate further up the CDG.
         */
bool CDGNode::PropagateLowerBoundTag(CDGElement* child_pt)
{
    
    if(opt_child != child_pt)  /// new child value > old child value > value. and "old_child_value > value" indicates that the value is not decided by this child.
    {
        return false;
    }
    else  /// child is the original opt next hop
    {
        is_exact_value = false;
        return true;
    }
}

