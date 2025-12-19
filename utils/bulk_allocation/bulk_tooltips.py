"""
Bulk Allocation Tooltips
========================
Centralized tooltip definitions for consistent UX.
Provides explanations for metrics, formulas, and UI elements.
"""

# ==================== STEP 1: SCOPE ====================

SCOPE_TOOLTIPS = {
    'products': """
**Products (SKU)**

Number of unique products (SKUs) with pending OCs in the selected scope.

Each product may appear in multiple OCs.
""",
    
    'total_ocs': """
**Total OCs**

Total number of OC detail lines in scope, including:
- Not allocated OCs
- Partially allocated OCs
- Fully allocated OCs

See breakdown below for details.
""",
    
    'need_allocation': """
**Need Allocation**

Number of OCs that need additional allocation.

```
Need Allocation = Total OCs - Fully Allocated
```

Includes:
- OCs never allocated
- Partially allocated OCs (can be topped up)
""",
    
    'fully_allocated': """
**Fully Allocated**

Number of OCs with sufficient allocation for pending delivery.

An OC is considered fully allocated when:
```
max_allocatable = 0
```

This means:
- `undelivered_allocated >= pending_qty`, OR
- `current_allocated >= effective_qty`
""",
    
    'not_allocated': """
**Not Allocated**

Number of OCs never allocated.

```
undelivered_allocated_qty = 0
```
""",
    
    'partially_allocated': """
**Partially Allocated**

Number of OCs with some allocation but not fully covered.

```
0 < undelivered_allocated < pending_qty
```

These OCs can receive additional top-up allocation.
""",
    
    'total_demand': """
**Total Demand**

Total pending delivery quantity for all OCs in scope.

```
= Σ pending_standard_delivery_quantity
```

This is the quantity customers are waiting to receive.
""",
    
    'allocatable_demand': """
**Allocatable Demand**

Quantity that can still be allocated.

```
= Σ max_allocatable (for OCs not fully allocated)
```

Where each OC:
```
max_allocatable = MIN(
    effective_qty - current_allocated,
    pending_qty - undelivered_allocated
)
```
""",
    
    'total_supply': """
**Total Supply**

Total supply from all sources:

```
Total Supply = Inventory + CAN Pending + PO Pending + WHT Pending
```

- **Inventory**: Current stock on hand
- **CAN Pending**: Container Arrival Notice awaiting stock-in
- **PO Pending**: Purchase Orders awaiting arrival
- **WHT Pending**: Warehouse Transfers in progress
""",
    
    'available_supply': """
**Available Supply**

Supply available after deducting committed quantity.

```
Available = Total Supply - Committed
```

**Committed** = Quantity already "committed" to pending OCs:
```
Committed = Σ MIN(pending_qty, undelivered_allocated)
```
""",
    
    'coverage': """
**Coverage %**

Ratio of available supply to allocatable demand.

```
Coverage = Available Supply / Allocatable Demand × 100%
```

- **≥100%**: Sufficient stock for all OCs needing allocation
- **<100%**: Stock shortage, strategy will allocate proportionally
""",
    
    'include_partial': """
**Include Partially Allocated OCs**

- ✅ **On**: Include OCs with prior allocation for top-up
- ❌ **Off**: Only OCs never allocated
""",
    
    'exclude_fully_allocated': """
**Exclude Fully Allocated OCs**

- ✅ **On** (recommended): Skip OCs that already have sufficient allocation
- ❌ **Off**: Show all OCs including fully allocated

Fully allocated OCs don't need additional allocation, so typically should be excluded.
""",
    
    'only_unallocated': """
**Only Unallocated OCs**

- ✅ **On**: Only show OCs never allocated
- ❌ **Off**: Include partially allocated OCs too
"""
}

# ==================== STEP 2: STRATEGY ====================

STRATEGY_TOOLTIPS = {
    'fcfs': """
**First Come First Serve (FCFS)**

Prioritizes OCs by creation date (oldest first).

✅ **Pros**: 
- Fair based on order sequence
- Easy to explain to customers

❌ **Cons**: 
- Doesn't consider delivery urgency
- Old OCs may no longer be urgent
""",
    
    'etd_priority': """
**ETD Priority**

Prioritizes OCs with the nearest ETD (Expected Time of Delivery).

✅ **Pros**: 
- Ensures delivery commitments
- Reduces late delivery risk

❌ **Cons**: 
- New OCs with urgent ETD may "jump the queue"
- Doesn't consider fairness by order date
""",
    
    'proportional': """
**Proportional**

Allocates based on each OC's demand proportion.

```
Allocation = (OC Demand / Total Demand) × Available Supply
```

✅ **Pros**: 
- Fair by volume
- Every OC receives some allocation

❌ **Cons**: 
- Small OCs may receive too little
- Doesn't consider urgency
""",
    
    'revenue_priority': """
**Revenue Priority**

Prioritizes OCs with highest value.

```
Priority Score = quantity × unit_price
```

✅ **Pros**: 
- Maximizes revenue coverage
- Protects revenue

❌ **Cons**: 
- Favors large customers / large orders
- May cause imbalance
""",
    
    'hybrid': """
**Hybrid Strategy (Recommended)**

Combines multiple strategies in phases:

1. **MIN_GUARANTEE (30%)**: Ensures each OC gets minimum allocation
2. **ETD_PRIORITY (40%)**: Prioritizes urgent deliveries  
3. **PROPORTIONAL (30%)**: Distributes remaining fairly

✅ Balances fairness, urgency, and coverage.
""",
    
    'allocation_mode': """
**Allocation Mode**

- **SOFT**: Flexible - system chooses best supply source
- **HARD**: Fixed - must specify exact supply source (Inventory, PO, etc.)

Bulk allocation typically uses **SOFT** mode.
""",
    
    'min_guarantee': """
**Minimum Guarantee %**

Minimum percentage each OC is guaranteed to receive in Hybrid strategy.

Example: **30%** = each OC receives at least 30% of its demand (if supply permits).

Ensures no OC is completely "starved".
""",
    
    'urgent_threshold': """
**Urgent Threshold (Days)**

OCs with ETD within N days are considered **urgent** and prioritized in the ETD_PRIORITY phase.

- Default: **7 days**
- Adjust based on your company's delivery lead time
"""
}

# ==================== STEP 3: REVIEW ====================

REVIEW_TOOLTIPS = {
    'demand_qty': """
**Demand Qty**

Pending delivery quantity for this OC.

```
= pending_standard_delivery_quantity
= standard_quantity - delivered_quantity
```

This is the quantity the customer is waiting to receive.
""",
    
    # RENAMED v3.0: current_allocated -> undelivered_allocated
    'undelivered_allocated': """
**Undelivered Allocated**

Quantity previously allocated but not yet delivered.

```
= undelivered_allocated_qty_standard
= allocated_qty - cancelled_qty - delivered_qty
```

This quantity has goods "committed" and will be delivered when shipment occurs.
""",
    
    # NEW v3.0: allocatable_qty tooltip
    'allocatable_qty': """
**Allocatable Qty**

Maximum quantity that can be allocated for this OC.

```
= allocatable_qty_standard
= MIN(
    Demand - Undelivered,    ← Rule 2: Delivery need
    OC Qty - Total Allocated  ← Rule 1: OC quota
)
```

This is the upper limit for Final Qty to prevent over-allocation.
""",
    
    # Legacy support
    'current_allocated': """
**Already Allocated**

Quantity previously allocated but not yet delivered.

```
= undelivered_allocated_qty_standard
```

This quantity has goods "committed" and will be delivered when shipment occurs.
""",
    
    'suggested_qty': """
**Suggested Qty**

Quantity the system suggests to allocate based on selected strategy.

```
= MIN(allocatable_qty, available_supply_share)
```

Can be adjusted in the **Final Qty** column if needed.
""",
    
    'final_qty': """
**Final Qty** ✏️

Quantity that will be allocated after commit.

⚠️ **Editable** - fine-tune before committing.

**Constraint**: Cannot exceed **Allocatable Qty** to prevent over-allocation.
""",
    
    'coverage_pct': """
**Coverage %**

Coverage ratio after allocation.

```
= (Current Allocated + Final Qty) / Demand Qty × 100%
```

Colors:
- 🟢 ≥80%: Good
- 🟡 50-79%: Medium  
- 🔴 <50%: Low
""",
    
    'allocated_etd': """
**Allocated ETD** ✏️

Expected delivery date for this allocation.

- **Default**: Taken from OC ETD
- **Editable** if delivery needs to be earlier/later than OC request

⚠️ If Allocated ETD > OC ETD: warning about delay will appear
""",
    
    'product_display': """
**Product Display**

Complete product information:

```
PT Code | Product Name | Package Size (Brand)
```

Example: P025000563 | 3M™ Glass Cloth Electrical Tape 69 | 19mmx33m (Vietape)
""",
    
    'over_allocation_warning': """
**⚠️ Over-allocation Warning**

Occurs when either condition is met:

1. **Commitment exceeds OC**: 
   `total_allocated > effective_qty`

2. **Over-allocated pending**: 
   `undelivered_allocated > pending_qty`

➡️ Review and adjust Final Qty before commit.
""",

    'split_allocation': """
**Split Allocation** ✂️

Allows splitting one OC line into multiple allocation records with different ETDs.

Use cases:
- Partial shipment with different dates
- Staged delivery planning
- Customer request for split delivery

Click "Add Split" to create additional allocation lines.
"""
}

# ==================== FORMULAS ====================

FORMULA_TOOLTIPS = {
    'max_allocatable': """
**Max Allocatable Calculation**

Formula for maximum quantity that can be allocated per OC:

```
max_allocatable = MAX(0, pending_qty - undelivered_allocated)
```

Where:
- `pending_qty` = OC quantity still needs delivery
- `undelivered_allocated` = allocation committed but not yet delivered

**Simplified**: How much more can be allocated = What's needed - What's already allocated
""",
    
    'committed_qty': """
**Committed Quantity**

Quantity already "committed" to existing OCs:

```
Committed = Σ MIN(pending_qty, undelivered_allocated)
```

Uses MIN because:
- If `pending < undelivered`: over-allocated, only need to deliver pending
- If `undelivered < pending`: only the allocated portion is locked
""",
    
    'available_supply': """
**Available Supply Calculation**

```
Total Supply = Inventory + CAN + PO + WHT
             = remaining_quantity (inventory)
             + pending_quantity (CAN)
             + pending_standard_arrival_quantity (PO)
             + transfer_quantity (WHT in-transit)

Committed = Σ MIN(pending_qty, undelivered_allocated)
            for all pending delivery OCs

Available = Total Supply - Committed
```
""",
    
    'coverage_calculation': """
**Coverage Calculation**

Coverage % = (undelivered_allocated / pending_qty) × 100%

Status based on coverage:
- 0% = Not Allocated
- 1-99% = Partially Allocated  
- 100% = Fully Allocated
- >100% = Over-Allocated (problem!)
"""
}

# ==================== ALLOCATION STATUS ====================

STATUS_TOOLTIPS = {
    'not_allocated': """
🔴 **Not Allocated**

OC has no pending allocation.
`undelivered_allocated = 0`

Action: Needs new allocation
""",
    
    'partially_allocated': """
🟡 **Partially Allocated**

OC has some allocation but doesn't fully cover pending need.
`0 < undelivered_allocated < pending_qty`

Action: Can be topped up with more allocation
""",
    
    'fully_allocated': """
🟢 **Fully Allocated**

OC has sufficient allocation for pending delivery.
`undelivered_allocated >= pending_qty`

Action: No allocation needed (skip or review to re-allocate)
""",
    
    'over_allocated': """
⚠️ **Over-Allocated**

OC has more allocation than pending delivery need.
`undelivered_allocated > pending_qty`

This is a problem! Actions:
- Cancel excess allocation
- Review if OC quantity was reduced
- Check for duplicate allocations
"""
}


# ==================== HELPER FUNCTION ====================

def get_tooltip(category: str, key: str) -> str:
    """
    Get tooltip text by category and key
    
    Args:
        category: One of 'scope', 'strategy', 'review', 'formula', 'status'
        key: Tooltip key within category
    
    Returns:
        Tooltip text or empty string if not found
    """
    tooltips = {
        'scope': SCOPE_TOOLTIPS,
        'strategy': STRATEGY_TOOLTIPS,
        'review': REVIEW_TOOLTIPS,
        'formula': FORMULA_TOOLTIPS,
        'status': STATUS_TOOLTIPS
    }
    return tooltips.get(category, {}).get(key, '')


def get_all_tooltips() -> dict:
    """Get all tooltips organized by category"""
    return {
        'scope': SCOPE_TOOLTIPS,
        'strategy': STRATEGY_TOOLTIPS,
        'review': REVIEW_TOOLTIPS,
        'formula': FORMULA_TOOLTIPS,
        'status': STATUS_TOOLTIPS
    }