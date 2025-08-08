"""
Smart rate limiting that adapts to connection pooling and API quotas.
"""

import time
import logging
from typing import Dict, Optional

log = logging.getLogger(__name__)

# Rate limiting state
_last_call_time: Dict[str, float] = {}
_call_count: Dict[str, int] = {}
_window_start: Dict[str, float] = {}


def smart_rate_limit(
    operation_type: str = "default",
    base_delay: float = 0.1,
    use_connection_pooling: bool = True,
    max_calls_per_minute: int = 100
) -> None:
    """
    Apply intelligent rate limiting based on various factors.
    
    Args:
        operation_type: Type of operation (e.g., 'metadata', 'batch_get', 'batch_update')
        base_delay: Base delay in seconds
        use_connection_pooling: Whether connection pooling is enabled
        max_calls_per_minute: Maximum calls per minute for this operation
    """
    now = time.time()
    
    # Initialize tracking for this operation
    if operation_type not in _last_call_time:
        _last_call_time[operation_type] = 0
        _call_count[operation_type] = 0
        _window_start[operation_type] = now
    
    # Calculate adaptive delay
    adaptive_delay = calculate_adaptive_delay(
        operation_type, base_delay, use_connection_pooling, now
    )
    
    # Check rate limits
    if should_rate_limit(operation_type, max_calls_per_minute, now):
        if adaptive_delay > 0:
            time.sleep(adaptive_delay)
            log.debug(f"⏱️ Smart rate limit: {operation_type} delayed {adaptive_delay:.3f}s")
    
    # Update tracking
    _last_call_time[operation_type] = now
    _call_count[operation_type] += 1


def calculate_adaptive_delay(
    operation_type: str,
    base_delay: float,
    use_connection_pooling: bool,
    current_time: float
) -> float:
    """Calculate adaptive delay based on various factors."""
    
    # With connection pooling, reduce delays significantly
    if use_connection_pooling:
        base_delay *= 0.3  # 70% reduction with pooling
    
    # Different delays for different operations
    operation_multipliers = {
        'metadata': 0.5,    # Metadata calls are lighter
        'batch_get': 0.8,   # Batch reads are medium weight
        'batch_update': 1.2, # Batch writes are heavier
        'worksheet_access': 0.3, # Worksheet metadata is very light
    }
    
    multiplier = operation_multipliers.get(operation_type, 1.0)
    adaptive_delay = base_delay * multiplier
    
    # Time-based adaptive delay (reduce delay if last call was long ago)
    last_call = _last_call_time.get(operation_type, 0)
    time_since_last = current_time - last_call
    
    if time_since_last > 5.0:  # More than 5 seconds ago
        adaptive_delay *= 0.5  # Reduce delay
    elif time_since_last > 1.0:  # More than 1 second ago
        adaptive_delay *= 0.8  # Slightly reduce delay
    
    return max(0, adaptive_delay)


def should_rate_limit(
    operation_type: str,
    max_calls_per_minute: int,
    current_time: float
) -> bool:
    """Determine if rate limiting should be applied."""
    
    window_start = _window_start[operation_type]
    call_count = _call_count[operation_type]
    
    # Reset window if more than a minute has passed
    if current_time - window_start > 60:
        _window_start[operation_type] = current_time
        _call_count[operation_type] = 0
        return True  # Always rate limit first call in new window
    
    # Check if we're approaching rate limits
    time_in_window = current_time - window_start
    calls_per_second = call_count / max(time_in_window, 1)
    
    # Rate limit if we're going too fast
    return calls_per_second > (max_calls_per_minute / 60) * 0.8  # 80% threshold


def reset_rate_limiting(operation_type: Optional[str] = None) -> None:
    """Reset rate limiting state for an operation or all operations."""
    if operation_type:
        _last_call_time.pop(operation_type, None)
        _call_count.pop(operation_type, None)
        _window_start.pop(operation_type, None)
    else:
        _last_call_time.clear()
        _call_count.clear()
        _window_start.clear()
    
    log.debug(f"🗑️ Rate limiting reset for: {operation_type or 'all operations'}")


def get_rate_limiting_stats() -> Dict[str, Dict]:
    """Get current rate limiting statistics."""
    now = time.time()
    stats = {}
    
    for op_type in _last_call_time.keys():
        window_start = _window_start.get(op_type, now)
        call_count = _call_count.get(op_type, 0)
        last_call = _last_call_time.get(op_type, 0)
        
        stats[op_type] = {
            'calls_in_window': call_count,
            'window_duration': now - window_start,
            'time_since_last_call': now - last_call,
            'calls_per_minute': (call_count / max(now - window_start, 1)) * 60
        }
    
    return stats