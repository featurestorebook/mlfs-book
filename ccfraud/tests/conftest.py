import sys
from unittest.mock import Mock, MagicMock
import builtins

# Create comprehensive mocks for hopsworks and hsfs modules
# These must be set before any ccfraud modules are imported

# Mock the transformation_statistics module
transformation_statistics_mock = MagicMock()
transformation_statistics_mock.TransformationStatistics = MagicMock()

# Mock hsfs module and submodules
hsfs_mock = MagicMock()
hsfs_mock.transformation_statistics = transformation_statistics_mock

# Mock hopsworks module
hopsworks_mock = MagicMock()

# Create a mock statistics object and inject it into builtins
# This is needed because cc_trans_fg.py line 24 references 'statistics' before it's defined
builtins.statistics = MagicMock()

# Create a mock decorator that returns the original function
def mock_udf(*args, **kwargs):
    """Mock the @hopsworks.udf decorator to return the function unchanged."""
    # The decorator is called with arguments: @hopsworks.udf(return_type, ...)
    # It should return a decorator that returns the function unchanged
    def decorator(func):
        return func
    return decorator

hopsworks_mock.udf = mock_udf

# Install the mocks before any imports
sys.modules['hsfs'] = hsfs_mock
sys.modules['hsfs.transformation_statistics'] = transformation_statistics_mock
sys.modules['hopsworks'] = hopsworks_mock
sys.modules['hopsworks.core'] = MagicMock()
