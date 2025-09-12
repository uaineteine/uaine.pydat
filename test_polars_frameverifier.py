#!/usr/bin/env python3
"""
Test script to verify FrameTypeVerifier works correctly with polars DataFrame and LazyFrame.

This test script verifies the fix for supporting both polars DataFrame and LazyFrame 
under the same 'polars' frame_type.
"""

import sys
import os

# Add the package to path for testing
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from uainepydat.frameverifier import FrameTypeVerifier
import polars as pl

def test_polars_support():
    """Test that both polars DataFrame and LazyFrame are supported"""
    
    # Create test data
    data = {'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']}
    
    # Test DataFrame
    df = pl.DataFrame(data)
    try:
        FrameTypeVerifier.verify(df, 'polars')
        print("✓ polars DataFrame verification: PASSED")
    except Exception as e:
        print(f"✗ polars DataFrame verification: FAILED - {e}")
        return False
    
    # Test LazyFrame
    lazy_df = df.lazy()
    try:
        FrameTypeVerifier.verify(lazy_df, 'polars')
        print("✓ polars LazyFrame verification: PASSED")
    except Exception as e:
        print(f"✗ polars LazyFrame verification: FAILED - {e}")
        return False
    
    # Test complex LazyFrame operations
    complex_lazy = df.lazy().filter(pl.col('col1') > 1).select(['col1'])
    try:
        FrameTypeVerifier.verify(complex_lazy, 'polars')
        print("✓ complex polars LazyFrame verification: PASSED")
    except Exception as e:
        print(f"✗ complex polars LazyFrame verification: FAILED - {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Testing polars DataFrame and LazyFrame support in FrameTypeVerifier")
    print("-" * 60)
    
    success = test_polars_support()
    
    print("-" * 60)
    if success:
        print("All tests PASSED! ✓")
        sys.exit(0)
    else:
        print("Some tests FAILED! ✗")
        sys.exit(1)