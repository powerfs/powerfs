#!/usr/bin/env python3
"""
PowerFS Conflict Resolution Agent

This agent automatically detects and resolves conflicts in PowerFS.
It demonstrates how to build an autonomous conflict resolution system.
"""

import time
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from powerfs import (
    PowerFSClient,
    ConflictType,
    ConflictResolution,
    MergePolicy,
    ConflictRecord,
    ConflictStats,
)
from powerfs.exceptions import PowerFSException, ConnectionError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("conflict_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ResolutionRule:
    """Rule for automatic conflict resolution."""
    conflict_type: ConflictType
    resolution: Optional[ConflictResolution]
    description: str
    requires_review: bool = False


class ConflictResolutionAgent:
    """
    Autonomous agent for detecting and resolving conflicts.
    
    Features:
    - Configurable resolution rules
    - Support for manual review queue
    - Statistics tracking
    - Rate limiting
    """

    def __init__(
        self,
        master_endpoint: str,
        api_key: Optional[str] = None,
        check_interval: int = 60,
        max_resolves_per_interval: int = 100,
        dry_run: bool = False
    ):
        """
        Initialize the agent.
        
        Args:
            master_endpoint: URL of PowerFS master service
            api_key: Optional API key for authentication
            check_interval: Seconds between conflict checks
            max_resolves_per_interval: Max conflicts to resolve per interval
            dry_run: If True, don't actually resolve conflicts
        """
        self.client = PowerFSClient(
            master_endpoint=master_endpoint,
            api_key=api_key,
            timeout=30,
            retries=3
        )
        self.check_interval = check_interval
        self.max_resolves_per_interval = max_resolves_per_interval
        self.dry_run = dry_run
        
        # Initialize statistics
        self.stats = AgentStatistics()
        
        # Configure resolution rules
        self.resolution_rules = self._configure_rules()
        
        # Review queue for conflicts requiring human attention
        self.review_queue: List[ConflictRecord] = []
        
        logger.info(f"Conflict Resolution Agent initialized")
        logger.info(f"  Master endpoint: {master_endpoint}")
        logger.info(f"  Check interval: {check_interval}s")
        logger.info(f"  Max resolves per interval: {max_resolves_per_interval}")
        logger.info(f"  Dry run mode: {dry_run}")

    def _configure_rules(self) -> Dict[ConflictType, ResolutionRule]:
        """Configure conflict resolution rules."""
        return {
            ConflictType.WRITE_WRITE: ResolutionRule(
                conflict_type=ConflictType.WRITE_WRITE,
                resolution=ConflictResolution.KEEP_LAST,
                description="Write-write conflict: Keep last version (LWW)",
                requires_review=False
            ),
            ConflictType.CREATE_CREATE: ResolutionRule(
                conflict_type=ConflictType.CREATE_CREATE,
                resolution=ConflictResolution.KEEP_ALL,
                description="Create-create conflict: Keep all versions",
                requires_review=False
            ),
            ConflictType.WRITE_UNLINK: ResolutionRule(
                conflict_type=ConflictType.WRITE_UNLINK,
                resolution=ConflictResolution.KEEP_FIRST,
                description="Write-unlink conflict: Keep first version",
                requires_review=False
            ),
            ConflictType.DELETE_CREATE: ResolutionRule(
                conflict_type=ConflictType.DELETE_CREATE,
                resolution=ConflictResolution.KEEP_LAST,
                description="Delete-create conflict: Keep the recreation",
                requires_review=False
            ),
            ConflictType.RENAME_CONFLICT: ResolutionRule(
                conflict_type=ConflictType.RENAME_CONFLICT,
                resolution=None,
                description="Rename conflict: Requires manual review",
                requires_review=True
            ),
        }

    def run(self):
        """Main agent loop."""
        logger.info("Starting Conflict Resolution Agent...")
        try:
            while True:
                self._process_interval()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            logger.info("Agent stopped by user")
        except Exception as e:
            logger.error(f"Agent fatal error: {e}", exc_info=True)
        finally:
            self.client.close()

    def _process_interval(self):
        """Process conflicts for one interval."""
        logger.info("=" * 60)
        logger.info(f"Processing interval #{self.stats.intervals_processed + 1}")
        logger.info("=" * 60)
        
        try:
            # Get conflict statistics
            stats = self.client.conflict_manager.get_stats()
            logger.info(f"Current stats: {stats.unresolved_count} unresolved, {stats.resolved_count} resolved")
            
            # Update agent statistics
            self.stats.intervals_processed += 1
            self.stats.total_conflicts_seen += stats.unresolved_count
            
            # Detect new conflicts
            new_conflicts = self._detect_new_conflicts()
            logger.info(f"Found {len(new_conflicts)} new conflicts")
            
            # Process conflicts
            if new_conflicts:
                self._process_conflicts(new_conflicts)
            
            # Log summary
            self._log_summary(stats)
            
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
        except PowerFSException as e:
            logger.error(f"PowerFS error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)

    def _detect_new_conflicts(self) -> List[ConflictRecord]:
        """Detect unresolved conflicts."""
        return self.client.conflict_manager.get_conflicts(unresolved_only=True)

    def _process_conflicts(self, conflicts: List[ConflictRecord]):
        """Process a batch of conflicts."""
        resolved_count = 0
        queued_for_review = 0
        skipped_count = 0
        
        for conflict in conflicts[:self.max_resolves_per_interval]:
            rule = self.resolution_rules.get(conflict.conflict_type)
            
            if not rule:
                logger.warning(f"No rule configured for conflict type: {conflict.conflict_type}")
                skipped_count += 1
                continue
            
            if rule.requires_review:
                self._queue_for_review(conflict)
                queued_for_review += 1
                continue
            
            if self._resolve_conflict(conflict, rule):
                resolved_count += 1
            else:
                skipped_count += 1
        
        # Update statistics
        self.stats.total_resolved += resolved_count
        self.stats.total_queued_for_review += queued_for_review
        self.stats.total_skipped += skipped_count
        
        logger.info(f"Conflict processing summary:")
        logger.info(f"  - Resolved: {resolved_count}")
        logger.info(f"  - Queued for review: {queued_for_review}")
        logger.info(f"  - Skipped: {skipped_count}")

    def _resolve_conflict(self, conflict: ConflictRecord, rule: ResolutionRule) -> bool:
        """Resolve a single conflict."""
        if not rule.resolution:
            logger.warning(f"No resolution strategy for conflict: {conflict.id}")
            return False
        
        if self.dry_run:
            logger.info(f"[DRY RUN] Would resolve conflict {conflict.id} using {rule.resolution.name}")
            return True
        
        try:
            logger.info(f"Resolving conflict {conflict.id} ({conflict.conflict_type.name})")
            logger.info(f"  Strategy: {rule.resolution.name}")
            logger.info(f"  Description: {rule.description}")
            
            success = self.client.conflict_manager.resolve_conflict(
                conflict.id,
                rule.resolution
            )
            
            if success:
                logger.info(f"  ✓ Success")
                self.stats.conflicts_resolved_by_type[conflict.conflict_type] += 1
                return True
            else:
                logger.error(f"  ✗ Failed")
                return False
                
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            self.stats.total_errors += 1
            return False

    def _queue_for_review(self, conflict: ConflictRecord):
        """Add conflict to review queue."""
        logger.info(f"Queuing conflict for review: {conflict.id}")
        self.review_queue.append(conflict)
        
        # Keep queue manageable (FIFO)
        if len(self.review_queue) > 1000:
            self.review_queue.pop(0)

    def _log_summary(self, stats: ConflictStats):
        """Log interval summary."""
        logger.info("-" * 60)
        logger.info(f"Agent Statistics:")
        logger.info(f"  Intervals processed: {self.stats.intervals_processed}")
        logger.info(f"  Total conflicts seen: {self.stats.total_conflicts_seen}")
        logger.info(f"  Total resolved: {self.stats.total_resolved}")
        logger.info(f"  Total queued for review: {self.stats.total_queued_for_review}")
        logger.info(f"  Total skipped: {self.stats.total_skipped}")
        logger.info(f"  Total errors: {self.stats.total_errors}")
        logger.info(f"  Resolution rate: {stats.resolution_rate:.1f}%")
        logger.info("-" * 60)

    def get_review_queue(self) -> List[ConflictRecord]:
        """Get conflicts waiting for manual review."""
        return self.review_queue

    def clear_review_queue(self):
        """Clear the review queue."""
        self.review_queue.clear()


class AgentStatistics:
    """Statistics tracking for the agent."""
    
    def __init__(self):
        self.intervals_processed = 0
        self.total_conflicts_seen = 0
        self.total_resolved = 0
        self.total_queued_for_review = 0
        self.total_skipped = 0
        self.total_errors = 0
        self.conflicts_resolved_by_type: Dict[ConflictType, int] = {
            ct: 0 for ct in ConflictType
        }


def main():
    """Example usage of the ConflictResolutionAgent."""
    import argparse
    
    parser = argparse.ArgumentParser(description="PowerFS Conflict Resolution Agent")
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
        "--interval",
        type=int,
        default=60,
        help="Check interval in seconds"
    )
    parser.add_argument(
        "--max-resolves",
        type=int,
        default=100,
        help="Max conflicts to resolve per interval"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (don't actually resolve)"
    )
    
    args = parser.parse_args()
    
    # Create and start agent
    agent = ConflictResolutionAgent(
        master_endpoint=args.master_endpoint,
        api_key=args.api_key,
        check_interval=args.interval,
        max_resolves_per_interval=args.max_resolves,
        dry_run=args.dry_run
    )
    
    agent.run()


if __name__ == "__main__":
    main()
