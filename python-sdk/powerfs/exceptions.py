class PowerFSException(Exception):
    """Base exception class for all PowerFS errors.
    
    All other exceptions in this module inherit from this class,
    allowing catch-all exception handling for PowerFS operations.
    """
    pass


class ConnectionError(PowerFSException):
    """Raised when there's a connection problem with the PowerFS master.
    
    This can occur when:
    - The master endpoint is unreachable
    - Network connectivity is lost
    - The master service is not responding
    - Authentication fails
    """
    pass


class ConflictNotFound(PowerFSException):
    """Raised when a specified conflict is not found.
    
    This occurs when trying to resolve or access a conflict that:
    - Has already been resolved
    - Never existed
    - Has been deleted from the system
    """
    pass


class ResolutionError(PowerFSException):
    """Raised when conflict resolution fails.
    
    This occurs when the master service is unable to resolve a conflict,
    possibly due to:
    - Invalid resolution strategy for the conflict type
    - Internal server error
    - Concurrent modification by another client
    """
    pass


class PolicyError(PowerFSException):
    """Raised when policy operations fail.
    
    This occurs when:
    - Setting an invalid policy
    - Policy propagation fails
    - Insufficient permissions to modify policy
    """
    pass
