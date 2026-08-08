"""Legacy import compatibility for the renamed non-parent order-state engine.

New code must import ``non_parent_order_state_engine`` directly.  This module
exists only so immutable historical analyses can still be reproduced.
"""

from scripts.factors.order_shape_mechanism.non_parent_order_state_engine import (
    FACTOR_VERSION,
    SIGNAL_GRID_SECONDS,
    NonParentOrderStateConfig,
    NonParentOrderStateEngine,
    NonParentOrderStateQuality,
)

BatchAConfig = NonParentOrderStateConfig
BatchAEngine = NonParentOrderStateEngine
BatchAQuality = NonParentOrderStateQuality
