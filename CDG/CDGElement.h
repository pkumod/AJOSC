
#ifndef CONJUNCTIVEQ_NODE_H
#define CONJUNCTIVEQ_NODE_H
#include "util.h"






#define changed_threshold 200


struct DegTracker;
struct JoinInfo
{
    vector<DegTracker*> related_deg_trackers;
    vector<double> related_deg_values; 
    int qv;
    JoinInfo(const set<int>& p, int qv_);

    JoinInfo() = default;
};


extern bool need_decide_order;

/**
 *
 * @param new_val
 * @param old_val
 * @param changed
 * @return same as bool UpdateDeg(double new_deg)'s return value.
 */

bool TestChanged(double new_val, double old_val, bool& changed);
struct CDGEdge;
struct DegTracker
{
    friend class CDG;
    double deg_actual = 0;
public:

    double deg_current = 0;
    bool deg_changed = false;
    int changed_num_oper = 0;

    vector<CDGEdge*> related_cdg_edges;



    bool BeforeHandleOrder();


    /**
     *
     * @param new_deg
     * @return whether deg become changed dramatically ("become" means, before this function, deg did not change dramatically).
     */
    bool UpdateDeg(double new_deg)
    {

        deg_actual = deg_actual*(1-theta)+ new_deg* theta;

        return TestChanged(deg_actual, deg_current, deg_changed);
    }

    void AfterHandleOrder();
};

struct CDGElement
{
    bool visited = false;
    bool visited_more_than_once = false;
    bool is_exact_value = true;
    double value = inf;
    vector<CDGElement*> parents;  ///< node's parent: edge whose dst is this node; edge's parent: the src node of this edge.



    virtual bool IncrementalUpdateWithLowerBound(double,CDGElement* child_pt) = 0;
    virtual bool PropagateLowerBoundTag(CDGElement* child_pt) = 0;
    virtual void CompExactValue() = 0;

};


struct CDGEdge;
struct CDGNode: public CDGElement
{
    
    vector<CDGEdge*> children;  
    CDGEdge* opt_child=nullptr;  ///< indicates N(P) in the paper.
    vector<pair<double,CDGEdge*>> child_val2id;


  
    
    explicit CDGNode(const set<int>& valid_next_hops_);
    void LocalUpdate();
 
    bool IncrementalUpdateWithLowerBound(double new_choose_val ,CDGElement* child_pt) override;

    void CompExactValue();

/**
         *
         * @param old_child_value
         * @return whether we need to propagate further up the CDG.
         */
    bool PropagateLowerBoundTag(CDGElement* child_pt) override;


    
};


struct CDGEdge: public CDGElement
{
    
    double w_actual = 0.4;  ///< the actual w(P,u) in the paper
    double w_current = 0.4;  ///< the current w(P,u) in the paper
    bool w_changed = false;
    JoinInfo join_info;

    CDGNode* src_pt = nullptr;  ///< in the paper: the src of the edge
    CDGNode* dst_pt = nullptr;   ///< in the paper: the dst of the edge

    
    void UpdW();
    
    explicit CDGEdge(CDGNode* p, const vector<int>& parents, int next_hop);
    void CompExactValue();

    void LocalUpdate();

    bool IncrementalUpdateWithLowerBound(double,CDGElement* child_pt) override;
    bool PropagateLowerBoundTag(CDGElement* child_pt) override;



    void UpdStat();
    
   
};
#endif //CONJUNCTIVEQ_NODE_H
