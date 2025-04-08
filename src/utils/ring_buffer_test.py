import numpy as np
import threading
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import sys
import gc

from ring_buffer import RingBufferSingleDimFloat, RingBufferSingleDimInt, RingBufferTwoDimFloat, RingBufferTwoDimInt, RingBufferMultiDim

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_overflow_behavior():
    """Test behavior when repeatedly pushing to a full buffer"""
    logger.info("Testing overflow behavior")
    
    try:
        # Create a small buffer
        buffer = RingBufferSingleDimFloat(5)
        
        # Fill it
        for i in range(5):
            buffer.append(float(i))
        
        assert len(buffer) == 5, f"Expected length 5, got {len(buffer)}"
        assert buffer.is_full, "Buffer should be full"
        
        # Now push more items and check behavior
        for i in range(5, 15):
            buffer.append(float(i))
            assert len(buffer) == 5, f"Buffer length changed unexpectedly to {len(buffer)}"
        
        # Check if the oldest items were dropped
        arr = buffer.as_array()
        assert arr[0] == 10.0, f"Expected first element to be 10.0, got {arr[0]}"
        assert arr[-1] == 14.0, f"Expected last element to be 14.0, got {arr[-1]}"
        
        logger.info("Overflow test passed")
    except Exception as e:
        logger.error(f"Overflow test failed: {e}")
        raise

def test_memory_corruption():
    """Try to cause memory corruption by manipulating internal arrays"""
    logger.info("Testing potential memory corruption")
    
    try:
        buffer = RingBufferSingleDimFloat(10)
        for i in range(5):
            buffer.append(float(i))
        
        # Try to access the internal array directly and modify it
        try:
            # This should fail or be harmless
            buffer._array[0] = 999.0
            assert buffer._array[0] == 999.0, "Direct modification should work if exposed"
            
            # Check if it affected the logical view correctly
            arr = buffer.as_array()
            # The direct modification might not be visible through the as_array() view,
            # depending on implementation details
            logger.info(f"After direct modification, as_array returns: {arr}")
        except AttributeError:
            logger.info("Protected internal array - good!")
        
        # Test with very large values
        try:
            buffer.append(float(1e30))
            buffer.append(float(1e-30))
            assert 1e30 in buffer, "Large value should be stored correctly"
            assert 1e-30 in buffer, "Small value should be stored correctly"
            logger.info("Large value test passed")
        except Exception as e:
            logger.error(f"Large value test failed: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Memory corruption test failed: {e}")
        raise

def test_race_conditions():
    """Test for race conditions with multiple threads"""
    logger.info("Testing race conditions")
    
    buffer = RingBufferSingleDimFloat(100)
    error_detected = [False]  # Using list for mutable reference
    
    def writer():
        try:
            for i in range(1000):
                buffer.append(float(i))
                time.sleep(0.0001)  # Small sleep to increase chance of race condition
        except Exception as e:
            logger.error(f"Writer thread error: {e}")
            error_detected[0] = True
    
    def reader():
        try:
            for _ in range(1000):
                if not buffer.is_empty:
                    _ = buffer.as_array()  # Read the array
                    if buffer.is_empty:
                        # This would indicate a race condition - was not empty, then suddenly empty
                        logger.error("Race condition detected: Buffer was not empty, then empty")
                        error_detected[0] = True
                time.sleep(0.0001)
        except Exception as e:
            logger.error(f"Reader thread error: {e}")
            error_detected[0] = True
    
    def popper():
        try:
            for _ in range(500):
                if not buffer.is_empty:
                    buffer.popleft()
                time.sleep(0.0002)
        except Exception as e:
            logger.error(f"Popper thread error: {e}")
            error_detected[0] = True
    
    # Start multiple threads
    with ThreadPoolExecutor(max_workers=6) as executor:
        tasks = []
        tasks.append(executor.submit(writer))
        tasks.append(executor.submit(reader))
        tasks.append(executor.submit(popper))
        tasks.append(executor.submit(writer))
        tasks.append(executor.submit(reader))
        tasks.append(executor.submit(popper))
        
        # Wait for all tasks to complete
        for task in tasks:
            task.result()
    
    assert not error_detected[0], "Race condition or thread safety issues detected"
    logger.info("Thread safety test completed")

def test_edge_cases():
    """Test edge cases like empty pops, extreme indices, etc."""
    logger.info("Testing edge cases")
    
    # Test empty pop
    buffer = RingBufferSingleDimFloat(5)
    try:
        buffer.popleft()
        logger.error("Should have raised exception when popping from empty buffer")
        assert False, "Failed to catch empty pop"
    except (AssertionError, ValueError):
        logger.info("Correctly caught empty pop")
    
    # Test wrapping around multiple times
    buffer = RingBufferSingleDimFloat(3)
    for i in range(30):  # Do 10 complete cycles
        buffer.append(float(i))
    
    # Check if indices are still valid
    assert len(buffer) == 3, f"Buffer length should be 3, got {len(buffer)}"
    assert buffer.as_array()[0] == 27.0, f"First element should be 27.0, got {buffer.as_array()[0]}"
    
    # Force index to large values to test _fix_indices_
    buffer = RingBufferSingleDimFloat(5)
    for i in range(5):
        buffer.append(float(i))
    
    # Manipulate indices directly if possible
    try:
        old_left = buffer._left_index
        old_right = buffer._right_index
        
        # Push indices to very large values
        buffer._left_index = 1000000
        buffer._right_index = 1000005
        
        # Fix indices
        buffer._fix_indices_()
        
        # Should be back to small values
        assert buffer._left_index < buffer.capacity, f"Left index not fixed: {buffer._left_index}"
        assert buffer._right_index - buffer._left_index == 5, f"Length changed after fixing indices"
        
        # Restore original values to not break the buffer
        buffer._left_index = old_left
        buffer._right_index = old_right
        logger.info("Index manipulation test passed")
    except AttributeError:
        logger.info("Could not directly manipulate indices - they might be protected")
    
    logger.info("Edge case tests completed")

def test_type_safety():
    """Test type safety by inserting wrong types"""
    logger.info("Testing type safety")
    
    buffer = RingBufferSingleDimFloat(5)
    
    # Try to insert an integer (should be auto-converted to float)
    try:
        buffer.append(42)
        assert 42.0 in buffer, "Integer should be converted to float"
        logger.info("Integer conversion to float works as expected")
    except Exception as e:
        logger.error(f"Type conversion failed: {e}")
    
    # Try to insert a string (should fail)
    try:
        buffer.append("string")
        logger.error("Inserted string into float buffer - type safety issue!")
        assert False, "Type safety failed"
    except (TypeError, ValueError, AssertionError):
        logger.info("Correctly prevented string insertion into float buffer")
    
    # Try to mix types in MultiDim buffer
    multi_buffer = RingBufferMultiDim(5, dtype=np.float64)
    try:
        multi_buffer.append(1.0)
        multi_buffer.append(2)  # Should convert to float
        
        try:
            multi_buffer.append("string")  # Should fail
            logger.error("Inserted string into float MultiDim buffer - type safety issue!")
            assert False, "Type safety failed in MultiDim"
        except (TypeError, ValueError):
            logger.info("Correctly prevented string insertion into float MultiDim buffer")
            
    except Exception as e:
        logger.error(f"MultiDim type test failed: {e}")
        
    logger.info("Type safety tests completed")

def test_memory_leaks():
    """Test for memory leaks by creating and destroying many buffers"""
    logger.info("Testing for memory leaks")
    
    # Get initial memory usage
    gc.collect()  # Force garbage collection
    initial_objects = len(gc.get_objects())
    
    # Create and destroy many buffers
    for _ in range(1000):
        buffer = RingBufferSingleDimFloat(1000)
        for i in range(500):
            buffer.append(float(i))
        # Let buffer go out of scope
    
    # Force garbage collection
    gc.collect()
    final_objects = len(gc.get_objects())
    
    # Check if object count increased dramatically
    if final_objects > initial_objects * 1.5:  # 50% increase might indicate a leak
        logger.warning(f"Possible memory leak: Started with {initial_objects} objects, ended with {final_objects}")
    else:
        logger.info(f"No obvious memory leak: {initial_objects} initial objects, {final_objects} final objects")
    
    logger.info("Memory leak test completed")

def test_2d_buffer_specializations():
    """Test the 2D buffer specializations"""
    logger.info("Testing 2D buffer specializations")
    
    try:
        # Create a 2D float buffer
        buffer = RingBufferTwoDimFloat(5, 3)
        
        # Add some arrays
        for i in range(3):
            values = np.array([float(i), float(i+1), float(i+2)], dtype=np.float64)
            buffer.append(values)
        
        assert len(buffer) == 3, f"Expected length 3, got {len(buffer)}"
        
        # Try with wrong size array
        try:
            wrong_size = np.array([1.0, 2.0], dtype=np.float64)  # Only 2 elements
            buffer.append(wrong_size)
            logger.error("Appended wrong sized array - should have failed!")
            assert False, "Size check failed"
        except (AssertionError, ValueError):
            logger.info("Correctly caught wrong-sized array")
        
        # Test __contains__
        search = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        assert search in buffer, "Array should be found in buffer"
        
        # Test pop methods
        popped = buffer.pop()
        assert popped[0] == 2.0, f"Expected [2.0, 3.0, 4.0], got {popped}"
        
        popped = buffer.popleft()
        assert popped[0] == 0.0, f"Expected [0.0, 1.0, 2.0], got {popped}"
        
        logger.info("2D buffer tests passed")
    except Exception as e:
        logger.error(f"2D buffer test failed: {e}")
        raise

def test_all():
    """Run all tests"""
    try:
        test_overflow_behavior()
        test_memory_corruption()
        test_race_conditions()
        test_edge_cases()
        test_type_safety()
        test_memory_leaks()
        test_2d_buffer_specializations()
        logger.info("All tests completed")
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        raise

if __name__ == "__main__":
    test_all()