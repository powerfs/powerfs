from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable


class ConflictType(Enum):
    """Conflict type enumeration.
    
    Represents different types of conflicts that can occur in PowerFS:
    - CREATE_CREATE: Two clients create files with the same name in the same directory
    - WRITE_WRITE: Multiple clients write to the same file
    - WRITE_UNLINK: Write to a file that has been deleted
    - DELETE_CREATE: File is deleted then recreated
    - RENAME_CONFLICT: Concurrent rename operations on the same file
    """
    CREATE_CREATE = 0
    WRITE_WRITE = 1
    WRITE_UNLINK = 2
    DELETE_CREATE = 3
    RENAME_CONFLICT = 4


class ConflictResolution(Enum):
    """Conflict resolution strategies.
    
    Strategies for resolving conflicts:
    - KEEP_FIRST: Keep the first version encountered
    - KEEP_LAST: Keep the last version (Last Write Wins - LWW)
    - KEEP_ALL: Keep all versions with auto-renaming
    - MERGE: Attempt to merge content (for text files)
    """
    KEEP_FIRST = 0
    KEEP_LAST = 1
    KEEP_ALL = 2
    MERGE = 3


class MergePolicy(Enum):
    """Directory merge policy.
    
    Default merge policies for directories:
    - LWW: Last Write Wins - simple timestamp-based resolution
    - WRITE_PRIORITY: Prioritize write operations over other operations
    - AGGRESSIVE: Aggressive merging with minimal conflicts
    - CONSERVATIVE: Conservative approach requiring manual intervention
    """
    LWW = 0
    WRITE_PRIORITY = 1
    AGGRESSIVE = 2
    CONSERVATIVE = 3


@dataclass
class ConflictBranch:
    """Information about a single branch of a conflict.
    
    Each conflict has multiple branches representing different versions
    of the same file from different clients.
    """
    name: str
    client_id: int
    inode: int
    size: int
    mtime: datetime


@dataclass
class ConflictRecord:
    """Complete conflict record.
    
    Contains all information about a detected conflict, including
    the conflict type, location, branches, and resolution status.
    """
    id: str
    conflict_type: ConflictType
    path: Optional[str] = None
    inode: Optional[int] = None
    create_time: Optional[datetime] = None
    resolved: bool = False
    resolved_time: Optional[datetime] = None
    resolution: Optional[ConflictResolution] = None
    branches: Optional[List[ConflictBranch]] = None


@dataclass
class ConflictStats:
    """Statistics about conflicts.
    
    Provides aggregated statistics about conflicts in a directory or
    across the entire filesystem.
    """
    total_count: int = 0
    resolved_count: int = 0
    unresolved_count: int = 0
    
    create_create_count: int = 0
    create_create_resolved: int = 0
    
    write_write_count: int = 0
    write_write_resolved: int = 0
    
    write_unlink_count: int = 0
    write_unlink_resolved: int = 0
    
    delete_create_count: int = 0
    delete_create_resolved: int = 0
    
    rename_conflict_count: int = 0
    rename_conflict_resolved: int = 0

    @property
    def resolution_rate(self) -> float:
        """Calculate resolution rate as percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.resolved_count / self.total_count) * 100


class EventType(Enum):
    """Types of events that can be triggered in PowerFS.
    
    Events are used for real-time notifications and can be subscribed to.
    """
    CONFLICT_DETECTED = "conflict_detected"
    CONFLICT_RESOLVED = "conflict_resolved"
    NODE_STATUS_CHANGED = "node_status_changed"
    VOLUME_STATUS_CHANGED = "volume_status_changed"
    ALERT_TRIGGERED = "alert_triggered"
    SESSION_CREATED = "session_created"
    SESSION_DELETED = "session_deleted"


class NotificationLevel(Enum):
    """Severity levels for notifications.
    
    - INFO: Informational messages
    - WARNING: Potential issues that need attention
    - ERROR: Errors that may require action
    - CRITICAL: Critical issues that require immediate action
    """
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class EventRecord:
    """Represents an event that occurred in PowerFS.
    
    Events are generated when certain conditions are met and can be
    subscribed to for real-time notifications.
    """
    event_id: str
    event_type: EventType
    level: NotificationLevel
    message: str
    source: str
    source_id: str
    timestamp: datetime
    payload: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class WebhookConfig:
    """Configuration for a webhook notification endpoint.
    
    Webhooks allow external systems to receive real-time notifications
    when events occur in PowerFS.
    """
    webhook_id: str
    name: str
    url: str
    enabled: bool = True
    events: Optional[List[EventType]] = None
    level_filter: Optional[NotificationLevel] = None
    secret: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt."""
    delivery_id: str
    webhook_id: str
    event_id: str
    status: str
    response_code: Optional[int] = None
    response_time: Optional[float] = None
    error_message: Optional[str] = None
