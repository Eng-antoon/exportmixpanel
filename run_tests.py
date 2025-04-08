#!/usr/bin/env python
import unittest
import pytest
import sys
import os

def run_unittest_tests():
    """Run tests using unittest discovery."""
    print("Running tests with unittest discovery...")
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    test_runner = unittest.TextTestRunner(verbosity=2)
    result = test_runner.run(test_suite)
    return result.wasSuccessful()

def run_pytest_tests():
    """Run tests using pytest."""
    print("Running tests with pytest...")
    return pytest.main(['-v', 'tests'])

if __name__ == '__main__':
    # Make sure we're in the right directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Run tests with unittest by default
    if len(sys.argv) == 1 or sys.argv[1] == 'unittest':
        success = run_unittest_tests()
    # Run tests with pytest if specified
    elif sys.argv[1] == 'pytest':
        success = run_pytest_tests() == 0
    else:
        print(f"Unknown test runner: {sys.argv[1]}")
        print("Usage: python run_tests.py [unittest|pytest]")
        sys.exit(1)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1) 