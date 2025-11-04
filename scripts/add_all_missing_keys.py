#!/usr/bin/env python3
"""
Add all missing FreeSurfer CDE keys from a stats directory.

This script processes all .stats files in a directory and updates the
freesurfer-cdes.json file with any missing keys.
"""

import sys
import json
from pathlib import Path

# Add src to path
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "src"))

from segstats_jsonld.fsutils import read_stats


def add_all_missing_keys_from_directory(stats_dir):
    """Process all .stats files in a directory and add missing keys."""
    stats_dir = Path(stats_dir)
    cde_file = repo_root / "src/segstats_jsonld/segstats_jsonld/mapping_data/freesurfer-cdes.json"
    
    # Load current CDEs
    with open(cde_file, 'r') as f:
        original_cdes = json.load(f)
    
    original_count = original_cdes.get('count', 0)
    print(f"Processing stats directory: {stats_dir}")
    print(f"Current CDE count: {original_count}\n")
    
    # Get all .stats files
    stats_files = sorted(stats_dir.glob("*.stats"))
    
    if not stats_files:
        print(f"No .stats files found in {stats_dir}")
        return 0
    
    processed = 0
    errors = []
    
    for stats_file in stats_files:
        print(f"Processing: {stats_file.name}")
        try:
            # Read stats with error_on_new_key=False to add all keys
            measures, header = read_stats(str(stats_file), error_on_new_key=False)
            processed += 1
            print(f"  ✓ Success")
        except Exception as e:
            errors.append((stats_file.name, str(e)))
            print(f"  ✗ Error: {e}")
        print()
    
    # Load updated CDEs to see what was added
    with open(cde_file, 'r') as f:
        updated_cdes = json.load(f)
    
    new_count = updated_cdes.get('count', 0)
    keys_added = new_count - original_count
    
    print("="*70)
    print(f"Summary:")
    print(f"  Files processed: {processed}/{len(stats_files)}")
    print(f"  Keys added: {keys_added}")
    print(f"  New CDE count: {new_count}")
    
    if errors:
        print(f"\nErrors encountered ({len(errors)} files):")
        for filename, error in errors:
            print(f"  {filename}: {error}")
    
    print("="*70)
    
    return keys_added


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python add_all_missing_keys.py <stats_directory>")
        print("\nExample:")
        print("  python add_all_missing_keys.py /path/to/freesurfer/sub-xxx/stats/")
        sys.exit(1)
    
    stats_dir = Path(sys.argv[1])
    
    if not stats_dir.exists():
        print(f"Error: Directory not found: {stats_dir}")
        sys.exit(1)
    
    if not stats_dir.is_dir():
        print(f"Error: Not a directory: {stats_dir}")
        sys.exit(1)
    
    try:
        keys_added = add_all_missing_keys_from_directory(stats_dir)
        print(f"\n✓ Successfully added {keys_added} new keys to freesurfer-cdes.json")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
