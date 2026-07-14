#!/usr/bin/env python3
"""
PowerFS Notification Agent Example

This example demonstrates how to use the notification system in PowerFS SDK.
It shows how to:
1. Subscribe to events
2. Start a background listener
3. Handle different types of events
4. Create and manage webhooks
"""

import time
import argparse
from typing import Optional

from powerfs import (
    PowerFSClient,
    EventType,
    NotificationLevel,
    EventRecord,
    WebhookConfig,
)


class NotificationExample:
    """Example class demonstrating notification features."""
    
    def __init__(self, master_endpoint: str, api_key: Optional[str] = None):
        """Initialize the example."""
        self.client = PowerFSClient(
            master_endpoint=master_endpoint,
            api_key=api_key,
            timeout=30,
            retries=3
        )
        
        # Track event counts for demonstration
        self.event_counts = {event_type: 0 for event_type in EventType}
        self.total_events = 0
    
    def _handle_event(self, event: EventRecord):
        """Generic event handler."""
        self.total_events += 1
        self.event_counts[event.event_type] += 1
        
        print(f"\n{'='*60}")
        print(f"Event #{self.total_events}: {event.event_type.value}")
        print(f"{'='*60}")
        print(f"Event ID:    {event.event_id}")
        print(f"Level:       {event.level.value.upper()}")
        print(f"Source:      {event.source} ({event.source_id})")
        print(f"Message:     {event.message}")
        print(f"Timestamp:   {event.timestamp}")
        
        if event.payload:
            print(f"Payload:     {event.payload}")
        
        if event.metadata:
            print(f"Metadata:    {event.metadata}")
        
        print(f"{'='*60}")
    
    def _handle_conflict_detected(self, event: EventRecord):
        """Handle conflict detection events."""
        print("🔔 CONFLICT DETECTED!")
        self._handle_event(event)
        
        # Additional conflict-specific handling
        if event.payload:
            conflict_info = event.payload.get('conflict', {})
            print(f"Conflict Type: {conflict_info.get('type', 'unknown')}")
            print(f"Conflict Path: {conflict_info.get('path', 'unknown')}")
    
    def _handle_alert_triggered(self, event: EventRecord):
        """Handle alert events."""
        print("🚨 ALERT TRIGGERED!")
        self._handle_event(event)
        
        # Critical alerts should trigger immediate action
        if event.level == NotificationLevel.CRITICAL:
            print("⚠️  This is a CRITICAL alert - immediate action required!")
    
    def _handle_node_status(self, event: EventRecord):
        """Handle node status change events."""
        print("🔄 NODE STATUS CHANGE")
        self._handle_event(event)
        
        if event.payload:
            old_status = event.payload.get('old_status', 'unknown')
            new_status = event.payload.get('new_status', 'unknown')
            print(f"Status changed from '{old_status}' to '{new_status}'")
    
    def setup_subscriptions(self):
        """Set up event subscriptions."""
        print("Setting up event subscriptions...")
        
        # Subscribe to conflict events
        self.client.notification_manager.subscribe(
            EventType.CONFLICT_DETECTED,
            self._handle_conflict_detected
        )
        
        # Subscribe to alert events
        self.client.notification_manager.subscribe(
            EventType.ALERT_TRIGGERED,
            self._handle_alert_triggered
        )
        
        # Subscribe to node status events
        self.client.notification_manager.subscribe(
            EventType.NODE_STATUS_CHANGED,
            self._handle_node_status
        )
        
        # Subscribe to volume status events
        self.client.notification_manager.subscribe(
            EventType.VOLUME_STATUS_CHANGED,
            self._handle_event
        )
        
        # Subscribe to session events
        self.client.notification_manager.subscribe(
            EventType.SESSION_CREATED,
            self._handle_event
        )
        self.client.notification_manager.subscribe(
            EventType.SESSION_DELETED,
            self._handle_event
        )
        
        print("✓ All subscriptions set up")
    
    def run_listener(self, poll_interval: int = 5):
        """Run the event listener."""
        print(f"\nStarting event listener with {poll_interval}s interval...")
        print("Press Ctrl+C to stop")
        
        try:
            self.client.notification_manager.start_listening(
                poll_interval=poll_interval
            )
            
            # Keep the main thread alive
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\nStopping event listener...")
            self.client.notification_manager.stop_listening()
            
        finally:
            self.client.close()
            print("\nEvent listener stopped")
            self._print_summary()
    
    def _print_summary(self):
        """Print event summary."""
        print(f"\n{'='*60}")
        print("EVENT SUMMARY")
        print(f"{'='*60}")
        print(f"Total events received: {self.total_events}")
        for event_type, count in self.event_counts.items():
            if count > 0:
                print(f"  {event_type.value}: {count}")
        print(f"{'='*60}")
    
    def manage_webhooks(self, webhook_url: Optional[str] = None):
        """Demonstrate webhook management."""
        print("\n" + "="*60)
        print("WEBHOOK MANAGEMENT DEMONSTRATION")
        print("="*60)
        
        # Get existing webhooks
        print("\n1. Getting existing webhooks...")
        webhooks = self.client.notification_manager.get_webhooks()
        print(f"Found {len(webhooks)} webhooks")
        
        for webhook in webhooks:
            print(f"  - ID: {webhook.webhook_id}")
            print(f"    Name: {webhook.name}")
            print(f"    URL: {webhook.url}")
            print(f"    Enabled: {webhook.enabled}")
            print(f"    Events: {[e.value for e in (webhook.events or [])]}")
        
        # Create a new webhook if URL is provided
        if webhook_url:
            print("\n2. Creating a new webhook...")
            try:
                new_webhook = self.client.notification_manager.create_webhook(
                    name="Example Webhook",
                    url=webhook_url,
                    events=[
                        EventType.CONFLICT_DETECTED,
                        EventType.ALERT_TRIGGERED,
                        EventType.NODE_STATUS_CHANGED
                    ],
                    level_filter=NotificationLevel.WARNING,
                    secret="my-secret-key",
                    timeout=30,
                    max_retries=3
                )
                
                print(f"✓ Webhook created successfully")
                print(f"  ID: {new_webhook.webhook_id}")
                print(f"  Name: {new_webhook.name}")
                print(f"  URL: {new_webhook.url}")
                
                # Test the webhook
                print("\n3. Testing webhook...")
                if self.client.notification_manager.test_webhook(new_webhook.webhook_id):
                    print("✓ Webhook test successful")
                else:
                    print("✗ Webhook test failed")
                
                # Update the webhook
                print("\n4. Updating webhook...")
                updated_webhook = self.client.notification_manager.update_webhook(
                    webhook_id=new_webhook.webhook_id,
                    enabled=False
                )
                print(f"✓ Webhook updated, enabled: {updated_webhook.enabled}")
                
                # Delete the webhook
                print("\n5. Deleting webhook...")
                self.client.notification_manager.delete_webhook(new_webhook.webhook_id)
                print("✓ Webhook deleted successfully")
                
            except Exception as e:
                print(f"Error managing webhook: {e}")
    
    def get_recent_events(self, limit: int = 20):
        """Get and display recent events."""
        print("\n" + "="*60)
        print(f"RECENT EVENTS (last {limit})")
        print("="*60)
        
        try:
            events = self.client.notification_manager.get_events(
                limit=limit
            )
            
            print(f"Found {len(events)} events\n")
            
            for i, event in enumerate(events, 1):
                print(f"{i}. [{event.level.value.upper()}] {event.event_type.value}")
                print(f"   {event.message}")
                print(f"   {event.timestamp}\n")
                
        except Exception as e:
            print(f"Error getting events: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PowerFS Notification Agent Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--master-endpoint",
        default="http://localhost:9333",
        help="PowerFS master endpoint"
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key for authentication"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Event poll interval in seconds"
    )
    parser.add_argument(
        "--webhook-url",
        default=None,
        help="URL for webhook demonstration"
    )
    parser.add_argument(
        "--show-events",
        action="store_true",
        help="Show recent events before starting listener"
    )
    
    args = parser.parse_args()
    
    # Create example instance
    example = NotificationExample(
        master_endpoint=args.master_endpoint,
        api_key=args.api_key
    )
    
    # Show recent events if requested
    if args.show_events:
        example.get_recent_events(limit=10)
    
    # Demonstrate webhook management
    example.manage_webhooks(webhook_url=args.webhook_url)
    
    # Set up subscriptions and run listener
    example.setup_subscriptions()
    example.run_listener(poll_interval=args.poll_interval)


if __name__ == "__main__":
    main()
