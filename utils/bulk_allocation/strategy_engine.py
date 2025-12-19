"""
Strategy Engine for Bulk Allocation
====================================
Implements allocation strategies:
- FCFS (First Come First Serve): Allocate by OC date
- ETD_PRIORITY: Allocate urgent deliveries first
- PROPORTIONAL: Allocate based on order size ratio
- REVENUE_PRIORITY: Allocate highest value orders first
- HYBRID: Multi-phase allocation combining strategies

REFACTORED: 2024-12 - Fixed Hybrid strategy proportional phase over-allocation bug

REFACTORED v3.0: 2024-12 - Simplified allocation logic
    - Use allocatable_qty directly from view (single source of truth)
    - Clear field naming: total_effective_allocated, undelivered_allocated
    - AllocationResult with explicit fields for validation
"""
import logging
from typing import Dict, List, Any, Optional
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import pandas as pd

logger = logging.getLogger(__name__)


class StrategyType(Enum):
    """Available allocation strategies"""
    FCFS = "fcfs"
    ETD_PRIORITY = "etd_priority"
    PROPORTIONAL = "proportional"
    REVENUE_PRIORITY = "revenue_priority"
    HYBRID = "hybrid"


@dataclass
class StrategyConfig:
    """Configuration for allocation strategy"""
    strategy_type: StrategyType
    allocation_mode: str = "SOFT"  # SOFT or HARD
    
    # For HYBRID strategy
    phases: List[Dict] = field(default_factory=list)
    
    # Common settings
    min_allocation_qty: float = 0.01
    max_allocation_percent: float = 100.0  # Max % of effective OC qty
    
    # Hybrid settings
    min_guarantee_percent: float = 30.0  # Min guarantee for each OC
    urgent_threshold_days: int = 7  # Days to consider "urgent"


@dataclass
class AllocationResult:
    """Result of allocation for a single OC"""
    ocd_id: int
    product_id: int
    customer_code: str
    demand_qty: float          # pending_standard_delivery_quantity
    effective_qty: float       # standard_quantity (OC qty after cancellation)
    current_allocated: float   # total_effective_allocated (for OC quota check)
    undelivered_allocated: float  # undelivered_allocated (for display)
    allocatable_qty: float     # max allocatable from view
    suggested_qty: float       # algorithm suggestion
    final_qty: float           # after fine-tuning
    coverage_percent: float
    priority_score: float
    strategy_source: str       # which strategy/phase allocated this
    warnings: List[str] = field(default_factory=list)


class AllocationStrategy(ABC):
    """Abstract base class for allocation strategies"""
    
    @abstractmethod
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float], 
                 config: StrategyConfig) -> List[AllocationResult]:
        """
        Execute allocation logic
        
        Args:
            demands: DataFrame with OC data
            supply: Dict mapping product_id -> available_supply
            config: Strategy configuration
            
        Returns:
            List of AllocationResult
        """
        pass
    
    @abstractmethod
    def get_priority_score(self, row: pd.Series) -> float:
        """Calculate priority score for sorting"""
        pass
    
    def _calculate_max_allocatable(self, row: pd.Series, config: StrategyConfig) -> float:
        """
        Get maximum allocatable quantity for an OC.
        
        Uses pre-calculated allocatable_qty from view which already applies:
        - Rule 1: Cannot exceed OC quota (effective_qty - total_effective_allocated)
        - Rule 2: Cannot exceed pending delivery needs (pending_qty - undelivered_allocated)
        
        Args:
            row: DataFrame row with OC data
            config: Strategy configuration (max_allocation_percent reserved for future use)
        
        Returns:
            Maximum quantity that can be allocated (>= 0)
        """
        # Primary source: pre-calculated from view (single source of truth)
        allocatable_qty = float(row.get('allocatable_qty', 0))
        
        # Fallback calculation if view field not available (backward compatibility)
        if allocatable_qty == 0 and row.get('allocatable_qty') is None:
            effective_qty = float(row.get('effective_qty', 0))
            total_effective_allocated = float(row.get('total_effective_allocated', 0))
            pending_qty = float(row.get('pending_qty', 0))
            undelivered = float(row.get('undelivered_allocated', 0))
            
            # Rule 1: Cannot exceed OC quota
            max_by_oc = effective_qty * (config.max_allocation_percent / 100) - total_effective_allocated
            
            # Rule 2: Cannot exceed pending delivery needs
            max_by_pending = pending_qty - undelivered
            
            allocatable_qty = max(0, min(max_by_oc, max_by_pending))
        
        return allocatable_qty


class FCFSStrategy(AllocationStrategy):
    """First Come First Serve - allocate by OC creation date"""
    
    def get_priority_score(self, row: pd.Series) -> float:
        """Earlier OC date = higher priority (lower score)"""
        try:
            oc_date = pd.to_datetime(row.get('oc_date'))
            # Convert to days since epoch for scoring
            return (oc_date - pd.Timestamp('2020-01-01')).days
        except:
            return float('inf')
    
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float],
                 config: StrategyConfig) -> List[AllocationResult]:
        """Allocate by OC date (oldest first)"""
        results = []
        remaining_supply = supply.copy()
        
        # Sort by OC date ascending
        sorted_demands = demands.copy()
        sorted_demands['priority_score'] = sorted_demands.apply(self.get_priority_score, axis=1)
        sorted_demands = sorted_demands.sort_values(['product_id', 'priority_score'])
        
        for _, row in sorted_demands.iterrows():
            product_id = int(row['product_id'])
            available = remaining_supply.get(product_id, 0)
            max_allocatable = self._calculate_max_allocatable(row, config)
            
            # Allocate up to available supply
            suggested_qty = min(max_allocatable, available)
            
            if suggested_qty >= config.min_allocation_qty:
                remaining_supply[product_id] = available - suggested_qty
            else:
                suggested_qty = 0
            
            results.append(AllocationResult(
                ocd_id=int(row['ocd_id']),
                product_id=product_id,
                customer_code=row['customer_code'],
                demand_qty=float(row['pending_qty']),
                effective_qty=float(row['effective_qty']),
                current_allocated=float(row.get('total_effective_allocated', 0)),
                undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                allocatable_qty=float(row.get('allocatable_qty', max_allocatable)),
                suggested_qty=suggested_qty,
                final_qty=suggested_qty,
                coverage_percent=(suggested_qty / float(row['pending_qty']) * 100) if row['pending_qty'] > 0 else 0,
                priority_score=float(row['priority_score']),
                strategy_source='FCFS'
            ))
        
        return results


class ETDPriorityStrategy(AllocationStrategy):
    """ETD Priority - allocate urgent deliveries first"""
    
    def get_priority_score(self, row: pd.Series) -> float:
        """Earlier ETD = higher priority (lower score)"""
        try:
            etd = pd.to_datetime(row.get('etd'))
            return (etd - pd.Timestamp('2020-01-01')).days
        except:
            return float('inf')
    
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float],
                 config: StrategyConfig) -> List[AllocationResult]:
        """Allocate by ETD (earliest first)"""
        results = []
        remaining_supply = supply.copy()
        
        # Sort by ETD ascending
        sorted_demands = demands.copy()
        sorted_demands['priority_score'] = sorted_demands.apply(self.get_priority_score, axis=1)
        sorted_demands = sorted_demands.sort_values(['product_id', 'priority_score'])
        
        for _, row in sorted_demands.iterrows():
            product_id = int(row['product_id'])
            available = remaining_supply.get(product_id, 0)
            max_allocatable = self._calculate_max_allocatable(row, config)
            
            suggested_qty = min(max_allocatable, available)
            
            if suggested_qty >= config.min_allocation_qty:
                remaining_supply[product_id] = available - suggested_qty
            else:
                suggested_qty = 0
            
            results.append(AllocationResult(
                ocd_id=int(row['ocd_id']),
                product_id=product_id,
                customer_code=row['customer_code'],
                demand_qty=float(row['pending_qty']),
                effective_qty=float(row['effective_qty']),
                current_allocated=float(row.get('total_effective_allocated', 0)),
                undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                allocatable_qty=float(row.get('allocatable_qty', max_allocatable)),
                suggested_qty=suggested_qty,
                final_qty=suggested_qty,
                coverage_percent=(suggested_qty / float(row['pending_qty']) * 100) if row['pending_qty'] > 0 else 0,
                priority_score=float(row['priority_score']),
                strategy_source='ETD_PRIORITY'
            ))
        
        return results


class ProportionalStrategy(AllocationStrategy):
    """Proportional - allocate based on order size ratio"""
    
    def get_priority_score(self, row: pd.Series) -> float:
        """Larger orders = higher priority (higher score)"""
        return float(row.get('pending_qty', 0))
    
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float],
                 config: StrategyConfig) -> List[AllocationResult]:
        """Allocate proportionally based on demand ratio"""
        results = []
        
        # Group by product and allocate proportionally within each group
        for product_id, group in demands.groupby('product_id'):
            available = supply.get(int(product_id), 0)
            
            # Calculate total allocatable demand for this product
            total_demand = 0
            allocatable_rows = []
            
            for _, row in group.iterrows():
                max_alloc = self._calculate_max_allocatable(row, config)
                if max_alloc >= config.min_allocation_qty:
                    total_demand += float(row['pending_qty'])
                    allocatable_rows.append((row, max_alloc))
            
            # Allocate proportionally
            for row, max_alloc in allocatable_rows:
                demand = float(row['pending_qty'])
                
                if total_demand > 0:
                    ratio = demand / total_demand
                    proportional_share = available * ratio
                    suggested_qty = min(proportional_share, max_alloc)
                else:
                    suggested_qty = 0
                
                if suggested_qty < config.min_allocation_qty:
                    suggested_qty = 0
                
                results.append(AllocationResult(
                    ocd_id=int(row['ocd_id']),
                    product_id=int(product_id),
                    customer_code=row['customer_code'],
                    demand_qty=demand,
                    effective_qty=float(row['effective_qty']),
                    current_allocated=float(row.get('total_effective_allocated', 0)),
                    undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                    allocatable_qty=float(row.get('allocatable_qty', max_alloc)),
                    suggested_qty=suggested_qty,
                    final_qty=suggested_qty,
                    coverage_percent=(suggested_qty / demand * 100) if demand > 0 else 0,
                    priority_score=demand,
                    strategy_source='PROPORTIONAL'
                ))
            
            # Handle rows that couldn't be allocated
            for _, row in group.iterrows():
                if int(row['ocd_id']) not in [r.ocd_id for r in results]:
                    results.append(AllocationResult(
                        ocd_id=int(row['ocd_id']),
                        product_id=int(product_id),
                        customer_code=row['customer_code'],
                        demand_qty=float(row['pending_qty']),
                        effective_qty=float(row['effective_qty']),
                        current_allocated=float(row.get('total_effective_allocated', 0)),
                        undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                        allocatable_qty=float(row.get('allocatable_qty', 0)),
                        suggested_qty=0,
                        final_qty=0,
                        coverage_percent=0,
                        priority_score=0,
                        strategy_source='PROPORTIONAL',
                        warnings=['Cannot allocate: max allocatable < minimum']
                    ))
        
        return results


class RevenuePriorityStrategy(AllocationStrategy):
    """Revenue Priority - allocate highest value orders first"""
    
    def get_priority_score(self, row: pd.Series) -> float:
        """Higher revenue = higher priority (negative for DESC sort)"""
        return -float(row.get('outstanding_amount_usd', 0) or 0)
    
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float],
                 config: StrategyConfig) -> List[AllocationResult]:
        """Allocate by order value (highest first)"""
        results = []
        remaining_supply = supply.copy()
        
        # Sort by revenue descending
        sorted_demands = demands.copy()
        sorted_demands['priority_score'] = sorted_demands.apply(self.get_priority_score, axis=1)
        sorted_demands = sorted_demands.sort_values(['product_id', 'priority_score'])
        
        for _, row in sorted_demands.iterrows():
            product_id = int(row['product_id'])
            available = remaining_supply.get(product_id, 0)
            max_allocatable = self._calculate_max_allocatable(row, config)
            
            suggested_qty = min(max_allocatable, available)
            
            if suggested_qty >= config.min_allocation_qty:
                remaining_supply[product_id] = available - suggested_qty
            else:
                suggested_qty = 0
            
            results.append(AllocationResult(
                ocd_id=int(row['ocd_id']),
                product_id=product_id,
                customer_code=row['customer_code'],
                demand_qty=float(row['pending_qty']),
                effective_qty=float(row['effective_qty']),
                current_allocated=float(row.get('total_effective_allocated', 0)),
                undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                allocatable_qty=float(row.get('allocatable_qty', max_allocatable)),
                suggested_qty=suggested_qty,
                final_qty=suggested_qty,
                coverage_percent=(suggested_qty / float(row['pending_qty']) * 100) if row['pending_qty'] > 0 else 0,
                priority_score=-float(row['priority_score']),  # Convert back to positive
                strategy_source='REVENUE_PRIORITY'
            ))
        
        return results


class HybridStrategy(AllocationStrategy):
    """
    Hybrid Strategy - Multi-phase allocation
    
    Default phases:
    1. MIN_GUARANTEE (30%): Ensure minimum allocation for all OCs
    2. ETD_PRIORITY (40%): Prioritize urgent deliveries
    3. PROPORTIONAL (30%): Distribute remaining supply fairly
    
    FIXED: 2024-12 - Proportional phase now properly tracks consumed supply
    """
    
    def __init__(self):
        self.fcfs = FCFSStrategy()
        self.etd = ETDPriorityStrategy()
        self.proportional = ProportionalStrategy()
        self.revenue = RevenuePriorityStrategy()
    
    def get_priority_score(self, row: pd.Series) -> float:
        """Combined score based on phases"""
        return 0  # Not used directly in hybrid
    
    def allocate(self, demands: pd.DataFrame, supply: Dict[int, float],
                 config: StrategyConfig) -> List[AllocationResult]:
        """Execute multi-phase allocation"""
        
        # Initialize results dictionary keyed by ocd_id
        results_dict = {}
        remaining_supply = supply.copy()
        
        # Get phases from config or use defaults
        phases = config.phases if config.phases else [
            {'name': 'MIN_GUARANTEE', 'weight': 30},
            {'name': 'ETD_PRIORITY', 'weight': 40},
            {'name': 'PROPORTIONAL', 'weight': 30}
        ]
        
        # Calculate supply allocation per phase
        phase_supply = {}
        for phase in phases:
            phase_supply[phase['name']] = {
                pid: supply.get(pid, 0) * (phase['weight'] / 100)
                for pid in supply.keys()
            }
        
        # Track accumulated allocations per OC
        accumulated = {int(row['ocd_id']): 0.0 for _, row in demands.iterrows()}
        
        # Phase 1: Minimum Guarantee
        if any(p['name'] == 'MIN_GUARANTEE' for p in phases):
            phase_config = next(p for p in phases if p['name'] == 'MIN_GUARANTEE')
            guarantee_percent = config.min_guarantee_percent / 100
            
            for product_id, group in demands.groupby('product_id'):
                available = phase_supply.get('MIN_GUARANTEE', {}).get(int(product_id), 0)
                
                for _, row in group.iterrows():
                    ocd_id = int(row['ocd_id'])
                    max_alloc = self._calculate_max_allocatable(row, config)
                    demand = float(row['pending_qty'])
                    
                    # Guarantee: min of (guarantee_percent of demand, max_allocatable, available_share)
                    guaranteed = min(
                        demand * guarantee_percent,
                        max_alloc,
                        available / len(group) if len(group) > 0 else 0
                    )
                    
                    if guaranteed >= config.min_allocation_qty:
                        accumulated[ocd_id] = guaranteed
                        remaining_supply[int(product_id)] = remaining_supply.get(int(product_id), 0) - guaranteed
        
        # Phase 2: ETD Priority or other sequential strategies
        for phase in phases:
            if phase['name'] in ['ETD_PRIORITY', 'FCFS', 'REVENUE_PRIORITY']:
                for product_id, group in demands.groupby('product_id'):
                    available = remaining_supply.get(int(product_id), 0)
                    
                    # Sort by strategy
                    if phase['name'] == 'ETD_PRIORITY':
                        sorted_group = group.sort_values('etd')
                    elif phase['name'] == 'FCFS':
                        sorted_group = group.sort_values('oc_date')
                    else:  # REVENUE_PRIORITY
                        sorted_group = group.sort_values('outstanding_amount_usd', ascending=False)
                    
                    phase_budget = supply.get(int(product_id), 0) * (phase['weight'] / 100)
                    spent = 0
                    
                    for _, row in sorted_group.iterrows():
                        ocd_id = int(row['ocd_id'])
                        max_alloc = self._calculate_max_allocatable(row, config)
                        current = accumulated.get(ocd_id, 0)
                        remaining_need = max_alloc - current
                        
                        if remaining_need > 0 and available > 0 and spent < phase_budget:
                            alloc = min(remaining_need, available, phase_budget - spent)
                            if alloc >= config.min_allocation_qty:
                                accumulated[ocd_id] = current + alloc
                                remaining_supply[int(product_id)] -= alloc
                                available -= alloc
                                spent += alloc
        
        # Phase 3: Proportional distribution of remaining
        # FIXED: Now properly tracks consumed supply to prevent over-allocation
        if any(p['name'] == 'PROPORTIONAL' for p in phases):
            for product_id, group in demands.groupby('product_id'):
                available = remaining_supply.get(int(product_id), 0)
                
                if available < config.min_allocation_qty:
                    continue
                
                # Calculate remaining needs
                needs = []
                total_need = 0
                for _, row in group.iterrows():
                    ocd_id = int(row['ocd_id'])
                    max_alloc = self._calculate_max_allocatable(row, config)
                    current = accumulated.get(ocd_id, 0)
                    remaining_need = max(0, max_alloc - current)
                    needs.append((ocd_id, remaining_need))
                    total_need += remaining_need
                
                # Distribute proportionally
                # FIX: Track spent supply to prevent over-allocation
                if total_need > 0:
                    spent = 0  # NEW: Track how much supply we've consumed
                    
                    for ocd_id, need in needs:
                        if need > 0 and spent < available:
                            share = (need / total_need) * available
                            # Cap share to: OC need, remaining supply
                            share = min(share, need, available - spent)
                            
                            if share >= config.min_allocation_qty:
                                accumulated[ocd_id] = accumulated.get(ocd_id, 0) + share
                                spent += share  # Track what we used
                    
                    # Update remaining supply after proportional distribution
                    remaining_supply[int(product_id)] = available - spent
        
        # FINAL CAP: Ensure no allocation exceeds allocatable_qty from view
        for _, row in demands.iterrows():
            ocd_id = int(row['ocd_id'])
            allocatable = float(row.get('allocatable_qty', 0))
            # Fallback to calculated max_alloc if allocatable_qty not available
            if allocatable == 0 and row.get('allocatable_qty') is None:
                allocatable = self._calculate_max_allocatable(row, config)
            if accumulated.get(ocd_id, 0) > allocatable:
                accumulated[ocd_id] = allocatable
        
        # Build final results
        for _, row in demands.iterrows():
            ocd_id = int(row['ocd_id'])
            suggested = accumulated.get(ocd_id, 0)
            demand = float(row['pending_qty'])
            allocatable = float(row.get('allocatable_qty', 0))
            
            results_dict[ocd_id] = AllocationResult(
                ocd_id=ocd_id,
                product_id=int(row['product_id']),
                customer_code=row['customer_code'],
                demand_qty=demand,
                effective_qty=float(row['effective_qty']),
                current_allocated=float(row.get('total_effective_allocated', 0)),
                undelivered_allocated=float(row.get('undelivered_allocated', 0)),
                allocatable_qty=allocatable,
                suggested_qty=suggested,
                final_qty=suggested,
                coverage_percent=(suggested / demand * 100) if demand > 0 else 0,
                priority_score=0,
                strategy_source='HYBRID'
            )
        
        return list(results_dict.values())


class StrategyEngine:
    """
    Main engine for executing allocation strategies
    """
    
    STRATEGY_INFO = {
        StrategyType.FCFS: {
            'name': 'First Come First Serve',
            'description': 'Allocate by OC creation date (oldest first)',
            'icon': '📅',
            'best_for': 'Fair treatment, audit compliance'
        },
        StrategyType.ETD_PRIORITY: {
            'name': 'ETD Priority',
            'description': 'Allocate urgent deliveries first (nearest ETD)',
            'icon': '🚨',
            'best_for': 'Meeting delivery commitments'
        },
        StrategyType.PROPORTIONAL: {
            'name': 'Proportional',
            'description': 'Allocate based on order size ratio',
            'icon': '⚖️',
            'best_for': 'Fair distribution by volume'
        },
        StrategyType.REVENUE_PRIORITY: {
            'name': 'Revenue Priority',
            'description': 'Allocate highest value orders first',
            'icon': '💰',
            'best_for': 'Maximize revenue coverage'
        },
        StrategyType.HYBRID: {
            'name': 'Hybrid (Recommended)',
            'description': 'Multi-phase allocation combining strategies',
            'icon': '🎯',
            'best_for': 'Balanced approach'
        }
    }
    
    def __init__(self):
        self.strategies = {
            StrategyType.FCFS: FCFSStrategy(),
            StrategyType.ETD_PRIORITY: ETDPriorityStrategy(),
            StrategyType.PROPORTIONAL: ProportionalStrategy(),
            StrategyType.REVENUE_PRIORITY: RevenuePriorityStrategy(),
            StrategyType.HYBRID: HybridStrategy()
        }
    
    def simulate(self, demands: pd.DataFrame, supply_df: pd.DataFrame,
                 config: StrategyConfig) -> List[AllocationResult]:
        """
        Run allocation simulation without database commit
        
        Args:
            demands: DataFrame from BulkAllocationData.get_demands_in_scope()
            supply_df: DataFrame from BulkAllocationData.get_supply_by_products()
            config: Strategy configuration
            
        Returns:
            List of AllocationResult
        """
        if demands.empty:
            logger.warning("No demands provided for simulation")
            return []
        
        # Convert supply DataFrame to dict
        supply = {}
        if not supply_df.empty:
            for _, row in supply_df.iterrows():
                supply[int(row['product_id'])] = float(row['available'])
        
        # Get strategy
        strategy = self.strategies.get(config.strategy_type)
        if not strategy:
            logger.error(f"Unknown strategy type: {config.strategy_type}")
            return []
        
        # Execute allocation
        try:
            results = strategy.allocate(demands, supply, config)
            
            # Add warnings for edge cases
            for result in results:
                if result.suggested_qty == 0 and result.demand_qty > 0:
                    if supply.get(result.product_id, 0) <= 0:
                        result.warnings.append("No supply available for this product")
                    elif result.current_allocated >= result.effective_qty:
                        result.warnings.append("OC already fully allocated")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in allocation simulation: {e}")
            return []
    
    def get_strategy_info(self, strategy_type: StrategyType) -> Dict[str, Any]:
        """Get information about a strategy"""
        return self.STRATEGY_INFO.get(strategy_type, {})
    
    def get_all_strategies(self) -> Dict[StrategyType, Dict[str, Any]]:
        """Get information about all available strategies"""
        return self.STRATEGY_INFO
    
    def recalculate_with_adjustments(self, results: List[AllocationResult],
                                    adjustments: Dict[int, float],
                                    supply: Dict[int, float]) -> List[AllocationResult]:
        """
        Recalculate allocations after manual adjustments
        
        Args:
            results: Original allocation results
            adjustments: Dict mapping ocd_id -> new final_qty
            supply: Available supply by product
            
        Returns:
            Updated list of AllocationResult
        """
        # Apply adjustments
        for result in results:
            if result.ocd_id in adjustments:
                new_qty = adjustments[result.ocd_id]
                result.final_qty = new_qty
                result.coverage_percent = (new_qty / result.demand_qty * 100) if result.demand_qty > 0 else 0
                
                if new_qty != result.suggested_qty:
                    if new_qty > result.suggested_qty:
                        result.warnings.append(f"Increased from suggested {result.suggested_qty:.0f}")
                    else:
                        result.warnings.append(f"Reduced from suggested {result.suggested_qty:.0f}")
        
        # Validate total allocations per product don't exceed supply
        product_totals = {}
        for result in results:
            pid = result.product_id
            product_totals[pid] = product_totals.get(pid, 0) + result.final_qty
        
        for result in results:
            pid = result.product_id
            if product_totals.get(pid, 0) > supply.get(pid, 0):
                result.warnings.append("⚠️ Product total exceeds available supply")
        
        return results