#!/usr/bin/env python3
"""
Find all missing FreeSurfer CDE keys in a stats file.

This script temporarily disables the error_on_new_key check to collect
all missing keys from a FreeSurfer stats file.
"""

import sys
import json
from pathlib import Path

# Add src to path
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root / "src"))

from segstats_jsonld.fsutils import read_stats

def find_all_missing_keys(stats_file):
    """Find all missing keys in a stats file by disabling the error check."""
    cde_file = Path(__file__).parent.parent / "src/segstats_jsonld/segstats_jsonld/mapping_data/freesurfer-cdes.json"
    
    # Load current CDEs
    with open(cde_file, 'r') as f:
        original_cdes = json.load(f)
    
    original_count = original_cdes.get('count', 0)
    
    print(f"Analyzing: {stats_file}")
    print(f"Current CDE count: {original_count}\n")
    
    # Read stats with error_on_new_key=False to collect all keys
    try:
        measures, header = read_stats(stats_file, error_on_new_key=False)
        
        # Load updated CDEs to see what was added
        with open(cde_file, 'r') as f:
            updated_cdes = json.load(f)
        
        new_count = updated_cdes.get('count', 0)
        
        if new_count > original_count:
            print(f"Found {new_count - original_count} new keys:\n")
            
            # Find and display the new keys
            for key, value in updated_cdes.items():
                if key == 'count':
                    continue
                if value.get('id', '').lstrip('0') and int(value['id']) > original_count:
                    print(f"  {key}")
                    print(f"    ID: {value['id']}")
                    print(f"    Label: {value.get('label', 'N/A')}")
                    print(f"    Description: {value.get('description', 'N/A')}")
                    print()
        else:
            print("No new keys found - all keys are already in the CDE file.")
        
        # Restore original CDE file
        with open(cde_file, 'w') as f:
            json.dump(original_cdes, f, indent=2)
        
        return new_count - original_count
        
    except Exception as e:
        # Restore original CDE file on error
        with open(cde_file, 'w') as f:
            json.dump(original_cdes, f, indent=2)
        raise


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python find_all_missing_keys.py <path_to_stats_file>")
        sys.exit(1)
    
    stats_file = Path(sys.argv[1])
    
    if not stats_file.exists():
        print(f"Error: Stats file not found: {stats_file}")
        sys.exit(1)
    
    try:
        count = find_all_missing_keys(stats_file)
        sys.exit(0 if count >= 0 else 1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
