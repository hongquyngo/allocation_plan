# File uom_converter_cleaned.py
"""
UOM Converter Service for Allocation Module - Cleaned Version
Only keeps functions actually used in the UI
"""
import logging
from typing import Union, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class UOMConverter:
    """Service for handling UOM conversions"""
    
    def __init__(self):
        self.DEFAULT_RATIO = '1'
        self.EPSILON = 0.0001  # For float comparison
    
    def needs_conversion(self, conversion_ratio: Optional[str]) -> bool:
        """Check if UOM conversion is needed based on ratio"""
        try:
            if not conversion_ratio:
                return False
            
            ratio_str = str(conversion_ratio).strip()
            ratio_value = self.parse_ratio_to_float(ratio_str)
            
            return abs(ratio_value - 1.0) > self.EPSILON
            
        except Exception as e:
            logger.warning(f"Error checking conversion need for ratio '{conversion_ratio}': {e}")
            return False
    
    def parse_ratio_to_float(self, ratio_str: str) -> float:
        """Parse conversion ratio string to float"""
        try:
            if not ratio_str:
                return 1.0
            
            ratio_str = str(ratio_str).strip()
            
            # Handle fraction format (e.g., "100/1")
            if '/' in ratio_str:
                parts = ratio_str.split('/')
                if len(parts) == 2:
                    numerator = float(parts[0].strip())
                    denominator = float(parts[1].strip())
                    
                    if denominator == 0:
                        logger.error(f"Division by zero in ratio: {ratio_str}")
                        return 1.0
                    
                    return numerator / denominator
                else:
                    logger.warning(f"Invalid fraction format: {ratio_str}")
                    return 1.0
            
            return float(ratio_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing ratio '{ratio_str}': {e}")
            return 1.0
    
    def convert_quantity(self, 
                        quantity: float, 
                        from_type: str, 
                        to_type: str, 
                        conversion_ratio: str) -> float:
        """Convert quantity between UOM types"""
        try:
            if from_type == to_type:
                return quantity
            
            ratio = self.parse_ratio_to_float(conversion_ratio)
            
            if from_type == 'standard' and to_type == 'selling':
                return quantity / ratio if ratio != 0 else quantity
            elif from_type == 'selling' and to_type == 'standard':
                return quantity * ratio
            elif from_type == 'standard' and to_type == 'buying':
                return quantity / ratio if ratio != 0 else quantity
            elif from_type == 'buying' and to_type == 'standard':
                return quantity * ratio
            elif from_type == 'selling' and to_type == 'buying':
                logger.warning(f"Direct conversion from {from_type} to {to_type} - assuming same UOM")
                return quantity
            elif from_type == 'buying' and to_type == 'selling':
                logger.warning(f"Direct conversion from {from_type} to {to_type} - assuming same UOM")
                return quantity
            else:
                logger.error(f"Unknown conversion: {from_type} to {to_type}")
                return quantity
                
        except Exception as e:
            logger.error(f"Error converting quantity {quantity} from {from_type} to {to_type}: {e}")
            return quantity