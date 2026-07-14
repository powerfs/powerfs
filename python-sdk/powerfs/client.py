import json
import requests
import threading
import time
from typing import Optional, List, Dict, Any, Callable
from datetime import datetime
from dateutil import parser

from .types import (
    ConflictType,
    ConflictResolution,
    MergePolicy,
    ConflictBranch,
    ConflictRecord,
    ConflictStats,
    EventType,
    NotificationLevel,
    EventRecord,
    WebhookConfig,
    WebhookDelivery,
)
from .exceptions import (
    PowerFSException,
    ConnectionError,
    ConflictNotFound,
    ResolutionError,
    PolicyError,
)


class ConflictManager:
    """Conflict management class for PowerFS.
    
    Provides methods for detecting, viewing, and resolving conflicts
    in the PowerFS filesystem. This class should be accessed through
    PowerFSClient.conflict_manager.
    """
    
    def __init__(self, client: 'PowerFSClient'):
        self._client = client
    
    def get_conflicts(
        self,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        unresolved_only: bool = True
    ) -> List[ConflictRecord]:
        """Get a list of conflicts in a directory.
        
        Args:
            dir_path: Directory path to search for conflicts.
            dir_ino: Directory inode to search for conflicts (alternative to dir_path).
            unresolved_only: If True, only return unresolved conflicts.
        
        Returns:
            List of ConflictRecord objects representing the conflicts.
        
        Raises:
            ConnectionError: If connection to master fails.
            PowerFSException: If the API returns an error.
        """
        params = {
            'unresolved_only': unresolved_only
        }
        
        if dir_path:
            params['dir_path'] = dir_path
        elif dir_ino:
            params['dir_ino'] = dir_ino
        
        response = self._client._request('GET', '/api/conflicts', params=params)

        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))

        # 后端返回 {code, message, data: [...]}，从 data 字段读取冲突列表
        conflicts_data = response.get('data', response.get('conflicts', []))
        return [self._parse_conflict_record(c) for c in conflicts_data]
    
    def resolve_conflict(
        self,
        conflict_id: str,
        resolution: ConflictResolution,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None
    ) -> bool:
        """Resolve a single conflict.
        
        Args:
            conflict_id: The ID of the conflict to resolve.
            resolution: The resolution strategy to apply.
            dir_path: Directory path where the conflict exists.
            dir_ino: Directory inode where the conflict exists.
        
        Returns:
            True if the conflict was resolved successfully.
        
        Raises:
            ConflictNotFound: If the conflict doesn't exist.
            ResolutionError: If resolution fails.
            ConnectionError: If connection to master fails.
        """
        data = {
            'conflict_id': conflict_id,
            'resolution': resolution.value
        }
        
        if dir_path:
            data['dir_path'] = dir_path
        elif dir_ino:
            data['dir_ino'] = dir_ino
        
        response = self._client._request('POST', '/api/conflicts/resolve', data=data)
        
        if not response.get('success', False):
            error = response.get('error', 'Unknown error')
            if 'not found' in error.lower():
                raise ConflictNotFound(error)
            raise ResolutionError(error)
        
        return True
    
    def batch_detect(
        self,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        recursive: bool = False
    ) -> ConflictStats:
        """Batch detect conflicts in a directory.
        
        Args:
            dir_path: Directory path to scan.
            dir_ino: Directory inode to scan (alternative to dir_path).
            recursive: If True, scan subdirectories recursively.
        
        Returns:
            ConflictStats object with counts of detected conflicts.
        
        Raises:
            ConnectionError: If connection to master fails.
            PowerFSException: If the API returns an error.
        """
        params = {
            'recursive': recursive
        }
        
        if dir_path:
            params['dir_path'] = dir_path
        elif dir_ino:
            params['dir_ino'] = dir_ino
        
        response = self._client._request('GET', '/api/conflicts/batch-detect', params=params)

        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))

        # 后端返回 {code, message, data: {...}}，从 data 字段读取统计信息
        stats_data = response.get('data', response)
        return self._parse_conflict_stats(stats_data)
    
    def batch_resolve(
        self,
        policy: MergePolicy,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        recursive: bool = False,
        conflict_type: Optional[ConflictType] = None
    ) -> int:
        """Batch resolve conflicts using a specified policy.
        
        Args:
            policy: The merge policy to use for resolution.
            dir_path: Directory path to resolve conflicts in.
            dir_ino: Directory inode to resolve conflicts in.
            recursive: If True, resolve conflicts in subdirectories too.
            conflict_type: If specified, only resolve conflicts of this type.
        
        Returns:
            Number of conflicts resolved.
        
        Raises:
            ResolutionError: If resolution fails.
            ConnectionError: If connection to master fails.
        """
        data = {
            'policy': policy.value,
            'recursive': recursive
        }
        
        if dir_path:
            data['dir_path'] = dir_path
        elif dir_ino:
            data['dir_ino'] = dir_ino
        
        if conflict_type is not None:
            data['conflict_type'] = conflict_type.value
        
        response = self._client._request('POST', '/api/conflicts/batch-resolve', data=data)
        
        if not response.get('success', False):
            raise ResolutionError(response.get('error', 'Unknown error'))
        
        return response.get('resolved_count', 0)
    
    def get_stats(
        self,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        recursive: bool = False
    ) -> ConflictStats:
        """Get conflict statistics for a directory.
        
        Args:
            dir_path: Directory path to get stats for.
            dir_ino: Directory inode to get stats for.
            recursive: If True, include subdirectories in stats.
        
        Returns:
            ConflictStats object with detailed statistics.
        
        Raises:
            ConnectionError: If connection to master fails.
            PowerFSException: If the API returns an error.
        """
        params = {
            'recursive': recursive
        }
        
        if dir_path:
            params['dir_path'] = dir_path
        elif dir_ino:
            params['dir_ino'] = dir_ino
        
        response = self._client._request('GET', '/api/conflicts/stats', params=params)

        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))

        # 后端返回 {code, message, data: {...}}，从 data 字段读取统计信息
        stats_data = response.get('data', response)
        return self._parse_conflict_stats(stats_data)
    
    def batch_ignore(
        self,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        conflict_type: Optional[ConflictType] = None
    ) -> int:
        """Batch ignore conflicts (mark as resolved without action).
        
        Args:
            dir_path: Directory path to ignore conflicts in.
            dir_ino: Directory inode to ignore conflicts in.
            conflict_type: If specified, only ignore conflicts of this type.
        
        Returns:
            Number of conflicts ignored.
        
        Raises:
            ResolutionError: If ignoring fails.
            ConnectionError: If connection to master fails.
        """
        data = {}
        
        if dir_path:
            data['dir_path'] = dir_path
        elif dir_ino:
            data['dir_ino'] = dir_ino
        
        if conflict_type is not None:
            data['conflict_type'] = conflict_type.value
        
        response = self._client._request('POST', '/api/conflicts/batch-ignore', data=data)
        
        if not response.get('success', False):
            raise ResolutionError(response.get('error', 'Unknown error'))
        
        return response.get('ignored_count', 0)
    
    def _parse_conflict_record(self, data: dict) -> ConflictRecord:
        """Parse a conflict record from API response."""
        branches = None
        if 'branches' in data:
            branches = [
                ConflictBranch(
                    name=b.get('name', ''),
                    client_id=b.get('client_id', 0),
                    inode=b.get('inode', 0),
                    size=b.get('size', 0),
                    mtime=self._parse_datetime(b.get('mtime'))
                )
                for b in data['branches']
            ]
        
        return ConflictRecord(
            id=data.get('id', ''),
            conflict_type=ConflictType(data.get('conflict_type', 0)),
            path=data.get('path'),
            inode=data.get('inode'),
            create_time=self._parse_datetime(data.get('create_time')),
            resolved=data.get('resolved', False),
            resolved_time=self._parse_datetime(data.get('resolved_time')),
            resolution=ConflictResolution(data.get('resolution')) if data.get('resolution') else None,
            branches=branches
        )
    
    def _parse_conflict_stats(self, data: dict) -> ConflictStats:
        """Parse conflict stats from API response."""
        return ConflictStats(
            total_count=data.get('total_count', 0),
            resolved_count=data.get('resolved_count', 0),
            unresolved_count=data.get('unresolved_count', 0),
            create_create_count=data.get('create_create_count', 0),
            create_create_resolved=data.get('create_create_resolved', 0),
            write_write_count=data.get('write_write_count', 0),
            write_write_resolved=data.get('write_write_resolved', 0),
            write_unlink_count=data.get('write_unlink_count', 0),
            write_unlink_resolved=data.get('write_unlink_resolved', 0),
            delete_create_count=data.get('delete_create_count', 0),
            delete_create_resolved=data.get('delete_create_resolved', 0),
            rename_conflict_count=data.get('rename_conflict_count', 0),
            rename_conflict_resolved=data.get('rename_conflict_resolved', 0)
        )
    
    def _parse_datetime(self, timestamp: Optional[int]) -> Optional[datetime]:
        """Parse Unix timestamp to datetime."""
        if timestamp is None:
            return None
        return datetime.fromtimestamp(timestamp)


class PolicyManager:
    """Policy management class for PowerFS.
    
    Provides methods for managing merge policies on directories.
    This class should be accessed through PowerFSClient.policy_manager.
    """
    
    def __init__(self, client: 'PowerFSClient'):
        self._client = client
    
    def get_policy(
        self,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None
    ) -> MergePolicy:
        """Get the merge policy for a directory.
        
        Args:
            dir_path: Directory path to get policy for.
            dir_ino: Directory inode to get policy for.
        
        Returns:
            The current merge policy for the directory.
        
        Raises:
            ConnectionError: If connection to master fails.
            PolicyError: If policy retrieval fails.
        """
        params = {}
        
        if dir_path:
            params['dir_path'] = dir_path
        elif dir_ino:
            params['dir_ino'] = dir_ino
        
        response = self._client._request('GET', '/api/policy', params=params)
        
        if not response.get('success', False):
            raise PolicyError(response.get('error', 'Unknown error'))
        
        policy_value = response.get('policy', 0)
        return MergePolicy(policy_value)
    
    def set_policy(
        self,
        policy: MergePolicy,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None,
        recursive: bool = False
    ) -> bool:
        """Set the merge policy for a directory.
        
        Args:
            policy: The merge policy to set.
            dir_path: Directory path to set policy on.
            dir_ino: Directory inode to set policy on.
            recursive: If True, apply to all subdirectories.
        
        Returns:
            True if policy was set successfully.
        
        Raises:
            PolicyError: If policy setting fails.
            ConnectionError: If connection to master fails.
        """
        data = {
            'policy': policy.value,
            'recursive': recursive
        }
        
        if dir_path:
            data['dir_path'] = dir_path
        elif dir_ino:
            data['dir_ino'] = dir_ino
        
        response = self._client._request('POST', '/api/policy', data=data)
        
        if not response.get('success', False):
            raise PolicyError(response.get('error', 'Unknown error'))
        
        return True
    
    def auto_resolve(
        self,
        policy: MergePolicy,
        dir_path: Optional[str] = None,
        dir_ino: Optional[int] = None
    ) -> int:
        """Auto-resolve conflicts using a specified policy.
        
        Args:
            policy: The merge policy to use.
            dir_path: Directory path to auto-resolve.
            dir_ino: Directory inode to auto-resolve.
        
        Returns:
            Number of conflicts resolved.
        
        Raises:
            ResolutionError: If auto-resolution fails.
            ConnectionError: If connection to master fails.
        """
        data = {
            'policy': policy.value
        }
        
        if dir_path:
            data['dir_path'] = dir_path
        elif dir_ino:
            data['dir_ino'] = dir_ino
        
        response = self._client._request('POST', '/api/conflicts/auto-resolve', data=data)
        
        if not response.get('success', False):
            raise ResolutionError(response.get('error', 'Unknown error'))
        
        return response.get('resolved_count', 0)


class NotificationManager:
    """Notification management class for PowerFS.
    
    Provides methods for subscribing to events, managing webhooks,
    and receiving real-time notifications about conflicts and system events.
    
    This class should be accessed through PowerFSClient.notification_manager.
    """
    
    def __init__(self, client: 'PowerFSClient'):
        self._client = client
        self._subscribers: Dict[EventType, List[Callable[[EventRecord], None]]] = {}
        self._listening = False
        self._listener_thread: Optional[threading.Thread] = None
        self._listener_stop_event = threading.Event()
        self._poll_interval = 5  # Default poll interval in seconds
    
    def subscribe(
        self,
        event_type: EventType,
        callback: Callable[[EventRecord], None]
    ):
        """Subscribe to events of a specific type.
        
        Args:
            event_type: The type of event to subscribe to.
            callback: The function to call when the event occurs.
                The callback receives an EventRecord object.
        
        Example:
            >>> def handle_conflict(event):
            ...     print(f"Conflict detected: {event.event_id}")
            ...
            >>> manager.subscribe(EventType.CONFLICT_DETECTED, handle_conflict)
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)
        print(f"Subscribed to {event_type.value} events")
    
    def unsubscribe(
        self,
        event_type: EventType,
        callback: Callable[[EventRecord], None]
    ):
        """Unsubscribe from events of a specific type.
        
        Args:
            event_type: The type of event to unsubscribe from.
            callback: The callback function to remove.
        """
        if event_type in self._subscribers:
            self._subscribers[event_type].remove(callback)
            print(f"Unsubscribed from {event_type.value} events")
    
    def start_listening(
        self,
        poll_interval: int = 5,
        event_types: Optional[List[EventType]] = None
    ):
        """Start listening for events.
        
        This runs a background thread that polls for events and
        notifies subscribers.
        
        Args:
            poll_interval: Number of seconds between polls (default: 5).
            event_types: List of event types to listen for.
                If None, listens for all subscribed types.
        """
        if self._listening:
            print("Already listening for events")
            return
        
        self._poll_interval = poll_interval
        self._listening = True
        self._listener_stop_event.clear()
        
        def listener_loop():
            while not self._listener_stop_event.is_set():
                try:
                    events = self._client._request(
                        'GET',
                        '/api/events',
                        params={
                            'types': ','.join([t.value for t in (event_types or self._subscribers.keys())])
                        }
                    )
                    
                    if events.get('success', False):
                        for event_data in events.get('events', []):
                            event = self._parse_event_record(event_data)
                            self._notify_subscribers(event)
                except Exception as e:
                    print(f"Error polling for events: {e}")
                
                time.sleep(self._poll_interval)
            
            self._listening = False
            print("Stopped listening for events")
        
        self._listener_thread = threading.Thread(target=listener_loop, daemon=True)
        self._listener_thread.start()
        print(f"Started listening for events with interval {poll_interval}s")
    
    def stop_listening(self):
        """Stop listening for events."""
        if not self._listening:
            print("Not listening for events")
            return
        
        self._listener_stop_event.set()
        if self._listener_thread:
            self._listener_thread.join()
    
    def _notify_subscribers(self, event: EventRecord):
        """Notify all subscribers of an event."""
        if event.event_type in self._subscribers:
            for callback in self._subscribers[event.event_type]:
                try:
                    callback(event)
                except Exception as e:
                    print(f"Error in event callback: {e}")
    
    def _parse_event_record(self, data: dict) -> EventRecord:
        """Parse an event record from API response."""
        return EventRecord(
            event_id=data.get('event_id', ''),
            event_type=EventType(data.get('event_type', 'unknown')),
            level=NotificationLevel(data.get('level', 'info')),
            message=data.get('message', ''),
            source=data.get('source', ''),
            source_id=data.get('source_id', ''),
            timestamp=parser.parse(data.get('timestamp')) if data.get('timestamp') else datetime.now(),
            payload=data.get('payload'),
            metadata=data.get('metadata')
        )
    
    def get_events(
        self,
        event_type: Optional[EventType] = None,
        level: Optional[NotificationLevel] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[EventRecord]:
        """Get a list of events from the server.
        
        Args:
            event_type: Filter by event type.
            level: Filter by notification level.
            limit: Maximum number of events to return.
            offset: Offset for pagination.
        
        Returns:
            List of EventRecord objects.
        """
        params: Dict[str, Any] = {
            'limit': limit,
            'offset': offset
        }
        
        if event_type:
            params['type'] = event_type.value
        
        if level:
            params['level'] = level.value
        
        response = self._client._request('GET', '/api/events', params=params)
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return [self._parse_event_record(e) for e in response.get('events', [])]
    
    def create_webhook(
        self,
        name: str,
        url: str,
        events: Optional[List[EventType]] = None,
        level_filter: Optional[NotificationLevel] = None,
        secret: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        max_retries: int = 3
    ) -> WebhookConfig:
        """Create a new webhook configuration.
        
        Args:
            name: Friendly name for the webhook.
            url: URL to send notifications to.
            events: List of event types to trigger this webhook.
            level_filter: Minimum notification level to trigger.
            secret: Secret for signing webhook payloads.
            headers: Custom headers to include in webhook requests.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retries.
        
        Returns:
            The created WebhookConfig object.
        """
        data: Dict[str, Any] = {
            'name': name,
            'url': url,
            'timeout': timeout,
            'max_retries': max_retries
        }
        
        if events:
            data['events'] = [e.value for e in events]
        
        if level_filter:
            data['level_filter'] = level_filter.value
        
        if secret:
            data['secret'] = secret
        
        if headers:
            data['headers'] = headers
        
        response = self._client._request('POST', '/api/webhooks', data=data)
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return self._parse_webhook_config(response.get('webhook', {}))
    
    def get_webhooks(self) -> List[WebhookConfig]:
        """Get all webhook configurations.
        
        Returns:
            List of WebhookConfig objects.
        """
        response = self._client._request('GET', '/api/webhooks')
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return [self._parse_webhook_config(w) for w in response.get('webhooks', [])]
    
    def update_webhook(
        self,
        webhook_id: str,
        **kwargs
    ) -> WebhookConfig:
        """Update an existing webhook configuration.
        
        Args:
            webhook_id: ID of the webhook to update.
            **kwargs: Fields to update (name, url, events, level_filter, etc.)
        
        Returns:
            The updated WebhookConfig object.
        """
        data: Dict[str, Any] = {}
        
        if 'name' in kwargs:
            data['name'] = kwargs['name']
        
        if 'url' in kwargs:
            data['url'] = kwargs['url']
        
        if 'enabled' in kwargs:
            data['enabled'] = kwargs['enabled']
        
        if 'events' in kwargs and kwargs['events']:
            data['events'] = [e.value for e in kwargs['events']]
        
        if 'level_filter' in kwargs:
            data['level_filter'] = kwargs['level_filter'].value
        
        if 'secret' in kwargs:
            data['secret'] = kwargs['secret']
        
        if 'headers' in kwargs:
            data['headers'] = kwargs['headers']
        
        if 'timeout' in kwargs:
            data['timeout'] = kwargs['timeout']
        
        if 'max_retries' in kwargs:
            data['max_retries'] = kwargs['max_retries']
        
        response = self._client._request(
            'PUT',
            f'/api/webhooks/{webhook_id}',
            data=data
        )
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return self._parse_webhook_config(response.get('webhook', {}))
    
    def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook configuration.
        
        Args:
            webhook_id: ID of the webhook to delete.
        
        Returns:
            True if deletion was successful.
        """
        response = self._client._request('DELETE', f'/api/webhooks/{webhook_id}')
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return True
    
    def get_webhook_deliveries(
        self,
        webhook_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[WebhookDelivery]:
        """Get webhook delivery records.
        
        Args:
            webhook_id: Filter by webhook ID.
            limit: Maximum number of records to return.
            offset: Offset for pagination.
        
        Returns:
            List of WebhookDelivery objects.
        """
        params: Dict[str, Any] = {
            'limit': limit,
            'offset': offset
        }
        
        if webhook_id:
            params['webhook_id'] = webhook_id
        
        response = self._client._request('GET', '/api/webhooks/deliveries', params=params)
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return [self._parse_webhook_delivery(d) for d in response.get('deliveries', [])]
    
    def _parse_webhook_config(self, data: dict) -> WebhookConfig:
        """Parse webhook config from API response."""
        events = data.get('events')
        return WebhookConfig(
            webhook_id=data.get('webhook_id', ''),
            name=data.get('name', ''),
            url=data.get('url', ''),
            enabled=data.get('enabled', True),
            events=[EventType(e) for e in events] if events else None,
            level_filter=NotificationLevel(data.get('level_filter')) if data.get('level_filter') else None,
            secret=data.get('secret'),
            headers=data.get('headers'),
            timeout=data.get('timeout', 30),
            max_retries=data.get('max_retries', 3)
        )
    
    def _parse_webhook_delivery(self, data: dict) -> WebhookDelivery:
        """Parse webhook delivery from API response."""
        return WebhookDelivery(
            delivery_id=data.get('delivery_id', ''),
            webhook_id=data.get('webhook_id', ''),
            event_id=data.get('event_id', ''),
            status=data.get('status', ''),
            response_code=data.get('response_code'),
            response_time=data.get('response_time'),
            error_message=data.get('error_message')
        )
    
    def test_webhook(self, webhook_id: str) -> bool:
        """Test a webhook by sending a test event.
        
        Args:
            webhook_id: ID of the webhook to test.
        
        Returns:
            True if the test was successful.
        """
        response = self._client._request('POST', f'/api/webhooks/{webhook_id}/test')
        
        if not response.get('success', False):
            raise PowerFSException(response.get('error', 'Unknown error'))
        
        return True


class PowerFSClient:
    """Main client class for interacting with PowerFS.
    
    This is the entry point for the PowerFS Python SDK. It provides
    access to conflict management and policy management functionality.
    
    Example:
        >>> client = PowerFSClient(master_endpoint="http://localhost:9333")
        >>> conflicts = client.conflict_manager.get_conflicts()
        >>> print(f"Found {len(conflicts)} conflicts")
    """
    
    def __init__(
        self,
        master_endpoint: str,
        api_key: Optional[str] = None,
        timeout: int = 30,
        retries: int = 3
    ):
        """Initialize the PowerFS client.
        
        Args:
            master_endpoint: URL of the PowerFS master service.
            api_key: Optional API key for authentication.
            timeout: Request timeout in seconds.
            retries: Number of retries for failed requests.
        """
        self._master_endpoint = master_endpoint.rstrip('/')
        self._api_key = api_key
        self._timeout = timeout
        self._retries = retries
        
        self._session = requests.Session()
        if api_key:
            self._session.headers.update({'Authorization': f'Bearer {api_key}'})
        
        self._conflict_manager = ConflictManager(self)
        self._policy_manager = PolicyManager(self)
        self._notification_manager = NotificationManager(self)
    
    @property
    def conflict_manager(self) -> ConflictManager:
        """Get the conflict manager instance."""
        return self._conflict_manager
    
    @property
    def policy_manager(self) -> PolicyManager:
        """Get the policy manager instance."""
        return self._policy_manager
    
    @property
    def notification_manager(self) -> NotificationManager:
        """Get the notification manager instance."""
        return self._notification_manager
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None
    ) -> dict:
        """Make an HTTP request to the PowerFS master.
        
        Args:
            method: HTTP method (GET, POST, etc.).
            endpoint: API endpoint path.
            params: Query parameters.
            data: Request body data.
        
        Returns:
            Parsed JSON response.
        
        Raises:
            ConnectionError: If connection fails.
        """
        url = f'{self._master_endpoint}{endpoint}'

        # 将 bool 参数转换为小写字符串（Python 默认会序列化为 True/False，
        # 而 Rust axum 只接受 true/false）
        if params:
            params = {
                k: ('true' if v is True else 'false' if v is False else v)
                for k, v in params.items()
            }

        for attempt in range(self._retries):
            try:
                response = self._session.request(
                    method,
                    url,
                    params=params,
                    json=data,
                    timeout=self._timeout
                )
                response.raise_for_status()
                result = response.json()
                # 后端返回 {code, message, data} 格式，这里补充 success 字段
                # 便于上层用 response.get('success') 判断成功
                if isinstance(result, dict) and 'success' not in result:
                    result['success'] = (result.get('code') == 200)
                    if not result['success']:
                        result.setdefault('error', result.get('message', 'Unknown error'))
                return result
            except requests.exceptions.RequestException as e:
                if attempt == self._retries - 1:
                    raise ConnectionError(f'Failed to connect to PowerFS: {str(e)}')
        
        raise ConnectionError('Failed to connect to PowerFS after retries')
    
    def close(self):
        """Close the HTTP session."""
        self._session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
