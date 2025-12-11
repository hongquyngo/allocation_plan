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

Số mã sản phẩm (SKU) có OC đang chờ giao trong phạm vi đã chọn.

Mỗi product có thể xuất hiện trong nhiều OC khác nhau.
""",
    
    'total_ocs': """
**Total OCs**

Tổng số dòng OC detail trong scope, bao gồm:
- OC chưa allocate
- OC đã allocate một phần
- OC đã fully allocated

Xem breakdown bên dưới để biết chi tiết.
""",
    
    'need_allocation': """
**Need Allocation**

Số OC cần được allocate thêm.

```
Need Allocation = Total OCs - Fully Allocated
```

Bao gồm:
- OC chưa allocate lần nào
- OC đã allocate một phần (có thể top-up)
""",
    
    'fully_allocated': """
**Fully Allocated**

Số OC đã có đủ allocation cho pending delivery.

OC được coi là fully allocated khi:
```
max_allocatable = 0
```

Tức là:
- `undelivered_allocated >= pending_qty`, HOẶC
- `current_allocated >= effective_qty`
""",
    
    'not_allocated': """
**Not Allocated**

Số OC chưa được allocate lần nào.

```
undelivered_allocated_qty = 0
```
""",
    
    'partially_allocated': """
**Partially Allocated**

Số OC đã có allocation nhưng chưa đủ.

```
0 < undelivered_allocated < pending_qty
```

Có thể top-up thêm allocation cho các OC này.
""",
    
    'total_demand': """
**Total Demand**

Tổng số lượng pending delivery của tất cả OCs trong scope.

```
= Σ pending_standard_delivery_quantity
```

Đây là số lượng khách hàng đang chờ nhận hàng.
""",
    
    'allocatable_demand': """
**Allocatable Demand**

Số lượng còn có thể allocate thêm.

```
= Σ max_allocatable (cho các OC chưa fully allocated)
```

Trong đó mỗi OC:
```
max_allocatable = MIN(
    effective_qty - current_allocated,
    pending_qty - undelivered_allocated
)
```
""",
    
    'total_supply': """
**Total Supply**

Tổng nguồn cung từ tất cả các nguồn:

```
Total Supply = Inventory + CAN Pending + PO Pending + WHT Pending
```

- **Inventory**: Hàng tồn kho hiện có
- **CAN Pending**: Container Arrival Notice chờ nhập
- **PO Pending**: Purchase Order chờ về
- **WHT Pending**: Warehouse Transfer chờ chuyển
""",
    
    'available_supply': """
**Available Supply**

Nguồn cung khả dụng sau khi trừ committed.

```
Available = Total Supply - Committed
```

**Committed** = Số lượng đã "cam kết" cho các OC pending:
```
Committed = Σ MIN(pending_qty, undelivered_allocated)
```
""",
    
    'coverage': """
**Coverage %**

Tỷ lệ nguồn cung so với nhu cầu allocatable.

```
Coverage = Available Supply / Allocatable Demand × 100%
```

- **≥100%**: Đủ hàng cho tất cả OCs cần allocate
- **<100%**: Thiếu hàng, strategy sẽ phân bổ hợp lý
""",
    
    'include_partial': """
**Include Partially Allocated OCs**

- ✅ **Bật**: Bao gồm OCs đã có allocation trước đó để top-up thêm
- ❌ **Tắt**: Chỉ OCs chưa allocate lần nào
""",
    
    'exclude_fully_allocated': """
**Exclude Fully Allocated OCs**

- ✅ **Bật** (khuyến nghị): Bỏ qua các OC đã có đủ allocation
- ❌ **Tắt**: Hiển thị tất cả OCs kể cả đã fully allocated

OC fully allocated không cần allocate thêm nên thường nên exclude.
""",
    
    'only_unallocated': """
**Only Unallocated OCs**

- ✅ **Bật**: Chỉ hiển thị OC chưa được allocate lần nào
- ❌ **Tắt**: Bao gồm cả OC đã partially allocated
"""
}

# ==================== STEP 2: STRATEGY ====================

STRATEGY_TOOLTIPS = {
    'fcfs': """
**First Come First Serve (FCFS)**

Ưu tiên OC theo ngày tạo (cũ nhất trước).

✅ **Ưu điểm**: 
- Công bằng theo thứ tự đặt hàng
- Dễ giải thích cho khách hàng

❌ **Nhược điểm**: 
- Không xét urgency của delivery date
- OC cũ có thể không còn urgent
""",
    
    'etd_priority': """
**ETD Priority**

Ưu tiên OC có ETD (Expected Time of Delivery) gần nhất.

✅ **Ưu điểm**: 
- Đảm bảo delivery commitment
- Giảm risk trễ hàng

❌ **Nhược điểm**: 
- OC mới với ETD gấp có thể "chen ngang"
- Không xét fairness theo thứ tự đặt
""",
    
    'proportional': """
**Proportional**

Phân bổ theo tỷ lệ demand của mỗi OC.

```
Allocation = (OC Demand / Total Demand) × Available Supply
```

✅ **Ưu điểm**: 
- Công bằng theo volume
- Mọi OC đều nhận được hàng

❌ **Nhược điểm**: 
- OC nhỏ có thể nhận số lượng quá ít
- Không xét urgency
""",
    
    'revenue_priority': """
**Revenue Priority**

Ưu tiên OC có giá trị cao nhất.

```
Priority Score = quantity × unit_price
```

✅ **Ưu điểm**: 
- Maximize revenue coverage
- Bảo vệ doanh thu

❌ **Nhược điểm**: 
- Thiên vị khách hàng lớn / đơn hàng lớn
- Có thể gây mất cân bằng
""",
    
    'hybrid': """
**Hybrid Strategy (Recommended)**

Kết hợp nhiều chiến lược theo phases:

1. **MIN_GUARANTEE (30%)**: Đảm bảo mỗi OC có tối thiểu
2. **ETD_PRIORITY (40%)**: Ưu tiên urgent deliveries  
3. **PROPORTIONAL (30%)**: Chia đều phần còn lại

✅ Cân bằng giữa fairness, urgency và coverage.
""",
    
    'allocation_mode': """
**Allocation Mode**

- **SOFT**: Flexible - system tự chọn nguồn supply tốt nhất
- **HARD**: Fixed - phải chỉ định cụ thể nguồn supply (Inventory, PO, etc.)

Bulk allocation thường dùng **SOFT** mode.
""",
    
    'min_guarantee': """
**Minimum Guarantee %**

Phần trăm tối thiểu mỗi OC được đảm bảo nhận trong Hybrid strategy.

Ví dụ: **30%** = mỗi OC nhận ít nhất 30% demand của nó (nếu supply đủ).

Giúp đảm bảo không có OC nào bị "bỏ đói" hoàn toàn.
""",
    
    'urgent_threshold': """
**Urgent Threshold (Days)**

OC có ETD trong vòng N ngày được coi là **urgent** và được ưu tiên trong ETD_PRIORITY phase.

- Default: **7 ngày**
- Điều chỉnh tùy theo lead time delivery của công ty
"""
}

# ==================== STEP 3: REVIEW ====================

REVIEW_TOOLTIPS = {
    'demand_qty': """
**Demand Qty**

Số lượng pending delivery của OC này.

```
= standard_quantity - delivered_quantity
```

Đây là số lượng khách hàng đang chờ nhận.
""",
    
    'current_allocated': """
**Already Allocated**

Số lượng đã được allocate trước đó nhưng chưa giao.

```
= undelivered_allocated_qty_standard
```

Phần này đã có "cam kết" hàng, sẽ được giao khi có delivery.
""",
    
    'suggested_qty': """
**Suggested Qty**

Số lượng system đề xuất allocate dựa trên strategy đã chọn.

Có thể điều chỉnh trong cột **Final Qty** nếu cần.
""",
    
    'final_qty': """
**Final Qty** ✏️

Số lượng sẽ được allocate sau khi commit.

⚠️ **Có thể edit** để fine-tune trước khi commit.

Lưu ý: Không nên vượt quá suggested qty trừ khi có lý do đặc biệt.
""",
    
    'coverage_pct': """
**Coverage %**

Tỷ lệ coverage sau allocation.

```
= (Current Allocated + Final Qty) / Demand Qty × 100%
```

Màu sắc:
- 🟢 ≥80%: Tốt
- 🟡 50-79%: Trung bình  
- 🔴 <50%: Thấp
""",
    
    'allocated_etd': """
**Allocated ETD** ✏️

Ngày dự kiến giao hàng cho allocation này.

- **Mặc định**: Lấy từ OC ETD
- **Có thể điều chỉnh** nếu cần giao sớm/muộn hơn OC yêu cầu

⚠️ Nếu Allocated ETD > OC ETD: sẽ có warning về delay
""",
    
    'product_display': """
**Product Display**

Hiển thị đầy đủ thông tin sản phẩm:

```
PT Code | Product Name | Package Size
```

Ví dụ: P022001923 | Adhesive Tape | 50mm x 100m
""",
    
    'over_allocation_warning': """
**⚠️ Over-allocation Warning**

Xảy ra khi một trong hai điều kiện:

1. **Commitment vượt OC**: 
   `total_allocated > effective_qty`

2. **Allocate thừa pending**: 
   `undelivered_allocated > pending_qty`

➡️ Kiểm tra và điều chỉnh Final Qty trước khi commit.
"""
}

# ==================== FORMULAS ====================

FORMULA_TOOLTIPS = {
    'max_allocatable': """
**Max Allocatable Calculation**

Công thức tính số lượng tối đa có thể allocate cho mỗi OC:

```
Rule 1: max_by_oc = effective_qty - current_allocated
        (Không vượt quá số lượng đặt hàng)

Rule 2: max_by_pending = pending_qty - undelivered_allocated  
        (Không allocate thừa so với cần giao)

max_allocatable = MIN(Rule 1, Rule 2)
```

Đảm bảo không over-allocate ở cả 2 chiều.
""",
    
    'committed_qty': """
**Committed Quantity**

Số lượng đã "cam kết" cho các OC hiện có:

```
Committed = Σ MIN(pending_qty, undelivered_allocated)
```

Lấy MIN vì:
- Nếu `pending < undelivered`: chỉ cần deliver pending
- Nếu `undelivered < pending`: chỉ committed phần đã allocate
""",
    
    'available_supply': """
**Available Supply Calculation**

```
Total Supply = Inventory + CAN + PO + WHT

Committed = Σ MIN(pending_qty, undelivered_allocated)
            cho tất cả OC pending delivery

Available = Total Supply - Committed
```
""",
    
    'coverage_calculation': """
**Coverage Calculation**

Có 2 cách tính coverage:

1. **Overall Coverage** (Total Demand):
```
Coverage = Available / Total Demand × 100%
```

2. **Allocatable Coverage** (Chỉ OC cần allocate):
```
Coverage = Available / Allocatable Demand × 100%
```

Allocatable coverage thường cao hơn vì exclude fully allocated OCs.
"""
}

# ==================== ALLOCATION STATUS ====================

STATUS_TOOLTIPS = {
    'not_allocated': """
🔴 **Not Allocated**

OC chưa có allocation nào.
`undelivered_allocated = 0`
""",
    
    'partially_allocated': """
🟡 **Partially Allocated**

OC đã có allocation nhưng chưa đủ cover pending.
`0 < undelivered_allocated < pending_qty`
""",
    
    'fully_allocated': """
🟢 **Fully Allocated**

OC đã có đủ allocation cho pending delivery.
`undelivered_allocated >= pending_qty` hoặc
`current_allocated >= effective_qty`
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