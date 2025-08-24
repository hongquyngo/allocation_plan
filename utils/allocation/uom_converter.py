"""
UOM Converter Service for Allocation Module
Handles all Unit of Measure conversions and validations
"""
import logging
from typing import Union, Optional, Tuple, Dict
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

logger = logging.getLogger(__name__)


class UOMConverter:
    """Service for handling UOM conversions"""
    
    def __init__(self):
        # Default values
        self.DEFAULT_RATIO = '1'
        self.EPSILON = 0.0001  # For float comparison
        
        # Common UOM mappings (for validation)
        self.COMMON_CONVERSIONS = {
            # Packaging units
            ('BOX', 'PCS'): 'varies',  # Depends on product
            ('CASE', 'BOX'): '12/1',   # Common case size
            ('PALLET', 'CASE'): '50/1', # Common pallet size
            
            # Weight units
            ('KG', 'G'): '1000/1',
            ('MT', 'KG'): '1000/1',
            ('LB', 'KG'): '0.453592',
            
            # Volume units
            ('L', 'ML'): '1000/1',
            ('GAL', 'L'): '3.78541',
            
            # Count units
            ('GROSS', 'PCS'): '144/1',  # 1 gross = 144 pieces
            ('DOZEN', 'PCS'): '12/1',
        }
    
    def needs_conversion(self, conversion_ratio: Optional[str]) -> bool:
        """
        Check if UOM conversion is needed based on ratio
        
        Args:
            conversion_ratio: The conversion ratio string (e.g., "100/1", "1", "1.5")
            
        Returns:
            True if conversion is needed (ratio != 1), False otherwise
        """
        try:
            # Handle None or empty
            if not conversion_ratio:
                return False
            
            # Clean the ratio string
            ratio_str = str(conversion_ratio).strip()
            
            # Parse the ratio to float
            ratio_value = self.parse_ratio_to_float(ratio_str)
            
            # Check if it's effectively 1 (within epsilon)
            return abs(ratio_value - 1.0) > self.EPSILON
            
        except Exception as e:
            logger.warning(f"Error checking conversion need for ratio '{conversion_ratio}': {e}")
            return False
    
    def parse_ratio_to_float(self, ratio_str: str) -> float:
        """
        Parse conversion ratio string to float
        
        Args:
            ratio_str: Ratio as string (e.g., "100/1", "1.5", "100")
            
        Returns:
            Float value of the ratio
        """
        try:
            # Handle None or empty
            if not ratio_str:
                return 1.0
            
            # Clean the string
            ratio_str = str(ratio_str).strip()
            
            # Handle fraction format (e.g., "100/1")
            if '/' in ratio_str:
                parts = ratio_str.split('/')
                if len(parts) == 2:
                    numerator = float(parts[0].strip())
                    denominator = float(parts[1].strip())
                    
                    # Avoid division by zero
                    if denominator == 0:
                        logger.error(f"Division by zero in ratio: {ratio_str}")
                        return 1.0
                    
                    return numerator / denominator
                else:
                    logger.warning(f"Invalid fraction format: {ratio_str}")
                    return 1.0
            
            # Handle decimal/integer format
            return float(ratio_str)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing ratio '{ratio_str}': {e}")
            return 1.0
    
    def convert_quantity(self, 
                        quantity: float, 
                        from_type: str, 
                        to_type: str, 
                        conversion_ratio: str) -> float:
        """
        Convert quantity between UOM types
        
        Args:
            quantity: The quantity to convert
            from_type: Source UOM type ('standard', 'selling', 'buying')
            to_type: Target UOM type ('standard', 'selling', 'buying')
            conversion_ratio: The conversion ratio (e.g., "100/1" means 100 standard = 1 selling)
            
        Returns:
            Converted quantity
        """
        try:
            # Same UOM type - no conversion needed
            if from_type == to_type:
                return quantity
            
            # Parse the conversion ratio
            ratio = self.parse_ratio_to_float(conversion_ratio)
            
            # Handle the conversions
            if from_type == 'standard' and to_type == 'selling':
                # Standard to Selling: divide by ratio
                # Example: 1000 TAB ÷ 100 = 10 BOX
                return quantity / ratio if ratio != 0 else quantity
                
            elif from_type == 'selling' and to_type == 'standard':
                # Selling to Standard: multiply by ratio
                # Example: 10 BOX × 100 = 1000 TAB
                return quantity * ratio
                
            elif from_type == 'standard' and to_type == 'buying':
                # Standard to Buying: divide by ratio
                # Similar to standard->selling
                return quantity / ratio if ratio != 0 else quantity
                
            elif from_type == 'buying' and to_type == 'standard':
                # Buying to Standard: multiply by ratio
                # Similar to selling->standard
                return quantity * ratio
                
            elif from_type == 'selling' and to_type == 'buying':
                # Selling to Buying: assume same (or would need separate ratio)
                logger.warning(f"Direct conversion from {from_type} to {to_type} - assuming same UOM")
                return quantity
                
            elif from_type == 'buying' and to_type == 'selling':
                # Buying to Selling: assume same (or would need separate ratio)
                logger.warning(f"Direct conversion from {from_type} to {to_type} - assuming same UOM")
                return quantity
                
            else:
                logger.error(f"Unknown conversion: {from_type} to {to_type}")
                return quantity
                
        except Exception as e:
            logger.error(f"Error converting quantity {quantity} from {from_type} to {to_type}: {e}")
            return quantity
    
    def format_ratio_display(self, ratio_str: str) -> str:
        """
        Format ratio for user-friendly display
        
        Args:
            ratio_str: Raw ratio string
            
        Returns:
            Formatted string for display
        """
        try:
            ratio_value = self.parse_ratio_to_float(ratio_str)
            
            # Special cases
            if ratio_value == 1.0:
                return "1:1"
            elif ratio_value == 1000.0:
                return "1000:1"
            elif ratio_value == 0.001:
                return "1:1000"
            
            # For fraction format, keep as is
            if '/' in str(ratio_str):
                return ratio_str
            
            # For decimal, format nicely
            if ratio_value.is_integer():
                return f"{int(ratio_value)}:1"
            else:
                # Try to find a nice fraction representation
                # This is simplified - could use fractions module for better results
                if ratio_value < 1:
                    inverse = 1 / ratio_value
                    if inverse.is_integer():
                        return f"1:{int(inverse)}"
                
                return f"{ratio_value:.3f}:1"
                
        except Exception:
            return str(ratio_str)
    
    def validate_conversion_consistency(self, 
                                      selling_qty: float,
                                      standard_qty: float,
                                      ratio_str: str,
                                      tolerance: float = 0.01) -> Tuple[bool, Optional[str]]:
        """
        Validate that quantities match the conversion ratio
        
        Args:
            selling_qty: Quantity in selling UOM
            standard_qty: Quantity in standard UOM
            ratio_str: Expected conversion ratio
            tolerance: Acceptable difference percentage (default 1%)
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            if selling_qty <= 0 or standard_qty <= 0:
                return True, None  # Skip validation for zero/negative quantities
            
            expected_ratio = self.parse_ratio_to_float(ratio_str)
            actual_ratio = standard_qty / selling_qty
            
            # Calculate percentage difference
            diff_percentage = abs(actual_ratio - expected_ratio) / expected_ratio
            
            if diff_percentage > tolerance:
                return False, (
                    f"Quantity mismatch: {selling_qty} selling × {expected_ratio} "
                    f"should equal {selling_qty * expected_ratio:.2f} standard, "
                    f"but got {standard_qty}"
                )
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error validating conversion: {e}")
            return False, f"Validation error: {str(e)}"
    
    def get_display_quantity_with_conversion(self,
                                           quantity: float,
                                           quantity_uom: str,
                                           alternate_uom: str,
                                           conversion_ratio: str,
                                           quantity_type: str = 'standard') -> Dict[str, str]:
        """
        Get formatted quantity strings with conversion
        
        Args:
            quantity: The quantity value
            quantity_uom: The UOM of the quantity
            alternate_uom: The alternate UOM to show
            conversion_ratio: The conversion ratio
            quantity_type: Type of the quantity ('standard' or 'selling')
            
        Returns:
            Dict with 'primary' and 'alternate' formatted strings
        """
        from .formatters import format_number
        
        result = {
            'primary': f"{format_number(quantity)} {quantity_uom}",
            'alternate': None,
            'needs_conversion': False
        }
        
        # Check if conversion is needed
        if self.needs_conversion(conversion_ratio):
            result['needs_conversion'] = True
            
            # Determine conversion direction
            if quantity_type == 'standard':
                # Convert standard to selling
                alternate_qty = self.convert_quantity(
                    quantity, 'standard', 'selling', conversion_ratio
                )
            else:
                # Convert selling to standard
                alternate_qty = self.convert_quantity(
                    quantity, 'selling', 'standard', conversion_ratio
                )
            
            result['alternate'] = f"{format_number(alternate_qty)} {alternate_uom}"
        
        return result
    
    def parse_uom_from_string(self, text: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Extract quantity and UOM from a string like "100 PCS" or "5.5 KG"
        
        Args:
            text: String containing quantity and UOM
            
        Returns:
            Tuple of (quantity, uom) or (None, None) if parsing fails
        """
        try:
            # Pattern to match number (including decimals) followed by text
            pattern = r'^([\d,]+\.?\d*)\s*(.+?)$'
            match = re.match(pattern, text.strip())
            
            if match:
                qty_str = match.group(1).replace(',', '')  # Remove commas
                uom_str = match.group(2).strip()
                
                quantity = float(qty_str)
                return quantity, uom_str
            
            return None, None
            
        except Exception as e:
            logger.error(f"Error parsing UOM from string '{text}': {e}")
            return None, None
    
    def is_compatible_uom(self, uom1: str, uom2: str) -> bool:
        """
        Check if two UOMs are potentially compatible for conversion
        
        Args:
            uom1: First UOM
            uom2: Second UOM
            
        Returns:
            True if UOMs are compatible
        """
        # Normalize UOMs
        uom1 = uom1.upper().strip()
        uom2 = uom2.upper().strip()
        
        # Same UOM is always compatible
        if uom1 == uom2:
            return True
        
        # Check known conversions
        if (uom1, uom2) in self.COMMON_CONVERSIONS or (uom2, uom1) in self.COMMON_CONVERSIONS:
            return True
        
        # Check by category
        categories = {
            'weight': ['KG', 'G', 'MG', 'MT', 'LB', 'OZ', 'TON'],
            'volume': ['L', 'ML', 'GAL', 'OZ', 'CUP', 'PINT', 'QUART'],
            'count': ['PCS', 'UNIT', 'EA', 'TAB', 'CAP'],
            'packaging': ['BOX', 'CASE', 'PACK', 'CARTON', 'PALLET', 'BAG', 'DRUM', 'PAIL'],
            'length': ['M', 'CM', 'MM', 'KM', 'FT', 'IN', 'YD'],
        }
        
        for category, units in categories.items():
            if uom1 in units and uom2 in units:
                return True
        
        return False
    
    def get_standard_uom_for_category(self, uom: str) -> str:
        """
        Get the standard UOM for a given UOM's category
        
        Args:
            uom: The UOM to check
            
        Returns:
            Standard UOM for that category
        """
        uom = uom.upper().strip()
        
        # Define standard UOMs for each category
        category_standards = {
            'weight': 'KG',
            'volume': 'L',
            'count': 'PCS',
            'packaging': 'PCS',  # Convert packaging to pieces
            'length': 'M',
        }
        
        # Find category
        categories = {
            'weight': ['KG', 'G', 'MG', 'MT', 'LB', 'OZ', 'TON'],
            'volume': ['L', 'ML', 'GAL', 'OZ', 'CUP', 'PINT', 'QUART'],
            'count': ['PCS', 'UNIT', 'EA', 'TAB', 'CAP'],
            'packaging': ['BOX', 'CASE', 'PACK', 'CARTON', 'PALLET', 'BAG', 'DRUM', 'PAIL'],
            'length': ['M', 'CM', 'MM', 'KM', 'FT', 'IN', 'YD'],
        }
        
        for category, units in categories.items():
            if uom in units:
                return category_standards.get(category, uom)
        
        # Default to PCS if unknown
        return 'PCS'


# Singleton instance
_converter_instance = None

def get_uom_converter() -> UOMConverter:
    """Get singleton instance of UOM converter"""
    global _converter_instance
    if _converter_instance is None:
        _converter_instance = UOMConverter()
    return _converter_instance


# Convenience functions
def needs_conversion(ratio: str) -> bool:
    """Check if conversion is needed"""
    return get_uom_converter().needs_conversion(ratio)

def convert_quantity(qty: float, from_type: str, to_type: str, ratio: str) -> float:
    """Convert quantity between UOM types"""
    return get_uom_converter().convert_quantity(qty, from_type, to_type, ratio)

def format_ratio(ratio: str) -> str:
    """Format ratio for display"""
    return get_uom_converter().format_ratio_display(ratio)