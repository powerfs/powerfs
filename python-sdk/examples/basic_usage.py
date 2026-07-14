#!/usr/bin/env python3
"""
PowerFS Python SDK - Basic Usage Example

This example demonstrates basic conflict management operations using the PowerFS SDK.
"""

from powerfs import PowerFSClient, ConflictType, MergePolicy, ConflictResolution


def main():
    # Initialize PowerFS client
    print("Initializing PowerFS client...")
    client = PowerFSClient(
        master_endpoint="http://localhost:9333",
        # api_key="your-api-key",  # Uncomment if authentication is required
        timeout=30,
        retries=3
    )
    print("Client initialized successfully")
    print()

    # Example 1: Get conflict statistics
    print("=" * 50)
    print("Example 1: Get Conflict Statistics")
    print("=" * 50)
    try:
        stats = client.conflict_manager.get_stats()
        print(f"Total conflicts: {stats.total_count}")
        print(f"Resolved conflicts: {stats.resolved_count}")
        print(f"Unresolved conflicts: {stats.unresolved_count}")
        print(f"Resolution rate: {stats.resolution_rate:.1f}%")
        print()
        print("By conflict type:")
        print(f"  - CREATE_CREATE: {stats.create_create_count} (resolved: {stats.create_create_resolved})")
        print(f"  - WRITE_WRITE: {stats.write_write_count} (resolved: {stats.write_write_resolved})")
        print(f"  - WRITE_UNLINK: {stats.write_unlink_count} (resolved: {stats.write_unlink_resolved})")
        print(f"  - DELETE_CREATE: {stats.delete_create_count} (resolved: {stats.delete_create_resolved})")
        print(f"  - RENAME_CONFLICT: {stats.rename_conflict_count} (resolved: {stats.rename_conflict_resolved})")
    except Exception as e:
        print(f"Error getting stats: {e}")
    print()

    # Example 2: Get unresolved conflicts
    print("=" * 50)
    print("Example 2: Get Unresolved Conflicts")
    print("=" * 50)
    try:
        conflicts = client.conflict_manager.get_conflicts(unresolved_only=True)
        print(f"Found {len(conflicts)} unresolved conflicts")
        
        for i, conflict in enumerate(conflicts[:5], 1):  # Show first 5 conflicts
            print(f"\nConflict #{i}:")
            print(f"  ID: {conflict.id}")
            print(f"  Type: {conflict.conflict_type.name}")
            print(f"  Path: {conflict.path or 'N/A'}")
            print(f"  Inode: {conflict.inode or 'N/A'}")
            print(f"  Created: {conflict.create_time or 'N/A'}")
            
            if conflict.branches:
                print(f"  Branches ({len(conflict.branches)}):")
                for j, branch in enumerate(conflict.branches, 1):
                    print(f"    Branch #{j}:")
                    print(f"      Name: {branch.name}")
                    print(f"      Client ID: {branch.client_id}")
                    print(f"      Size: {branch.size} bytes")
                    print(f"      Modified: {branch.mtime}")
    except Exception as e:
        print(f"Error getting conflicts: {e}")
    print()

    # Example 3: Resolve conflicts by type
    print("=" * 50)
    print("Example 3: Resolve Conflicts by Type")
    print("=" * 50)
    try:
        # Get all unresolved conflicts
        conflicts = client.conflict_manager.get_conflicts(unresolved_only=True)
        
        # Resolve WRITE_WRITE conflicts using KEEP_LAST strategy
        write_write_conflicts = [c for c in conflicts if c.conflict_type == ConflictType.WRITE_WRITE]
        print(f"Found {len(write_write_conflicts)} WRITE_WRITE conflicts")
        
        for conflict in write_write_conflicts[:3]:  # Resolve first 3
            print(f"Resolving conflict: {conflict.id}")
            success = client.conflict_manager.resolve_conflict(
                conflict.id,
                ConflictResolution.KEEP_LAST
            )
            if success:
                print(f"  ✓ Resolved successfully")
            else:
                print(f"  ✗ Failed to resolve")
    except Exception as e:
        print(f"Error resolving conflicts: {e}")
    print()

    # Example 4: Batch resolve conflicts
    print("=" * 50)
    print("Example 4: Batch Resolve Conflicts")
    print("=" * 50)
    try:
        print("Resolving conflicts in /data directory using LWW policy...")
        resolved_count = client.conflict_manager.batch_resolve(
            policy=MergePolicy.LWW,
            dir_path="/data",
            recursive=True
        )
        print(f"Batch resolved {resolved_count} conflicts")
    except Exception as e:
        print(f"Error batch resolving: {e}")
    print()

    # Example 5: Set merge policy for a directory
    print("=" * 50)
    print("Example 5: Set Merge Policy")
    print("=" * 50)
    try:
        print("Setting LWW policy for /documents directory (recursive)...")
        success = client.policy_manager.set_policy(
            policy=MergePolicy.LWW,
            dir_path="/documents",
            recursive=True
        )
        if success:
            print("✓ Policy set successfully")
        else:
            print("✗ Failed to set policy")
    except Exception as e:
        print(f"Error setting policy: {e}")
    print()

    # Example 6: Auto-resolve conflicts
    print("=" * 50)
    print("Example 6: Auto-Resolve Conflicts")
    print("=" * 50)
    try:
        print("Auto-resolving conflicts in /temp using LWW policy...")
        resolved_count = client.policy_manager.auto_resolve(
            policy=MergePolicy.LWW,
            dir_path="/temp"
        )
        print(f"Auto-resolved {resolved_count} conflicts")
    except Exception as e:
        print(f"Error auto-resolving: {e}")
    print()

    # Close the client
    print("Closing client...")
    client.close()
    print("Done!")


if __name__ == "__main__":
    main()
