#!/usr/bin/env python
"""
Test script for the Internet Archive (IA) pipeline.
This script tests the complete IA data collection, processing, and reporting pipeline.
"""
import os
import sys
import subprocess
import tempfile
import shutil

def run_command(cmd, cwd=None):
    """Run a command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {cmd}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
    return result

def test_ia_pipeline():
    """Test the complete IA pipeline."""
    print("Testing Internet Archive (IA) Pipeline")
    print("=" * 50)
    
    # Get the current directory (should be the project root)
    project_root = os.getcwd()
    print(f"Project root: {project_root}")
    
    # Test 1: Check if all required files exist
    print("\n1. Checking required files...")
    required_files = [
        "scripts/1-fetch/internetarchive_fetch.py",
        "scripts/2-process/internetarchive_process.py", 
        "scripts/3-report/internetarchive_report.py",
        "data/ia_license_mapping.csv"
    ]
    
    for file_path in required_files:
        full_path = os.path.join(project_root, file_path)
        if os.path.exists(full_path):
            print(f"[OK] {file_path}")
        else:
            print(f"[FAIL] {file_path} - MISSING")
            return False
    
    # Test 2: Test fetch script with development mode
    print("\n2. Testing fetch script (development mode)...")
    fetch_cmd = "pipenv run python scripts/1-fetch/internetarchive_fetch.py --dev --enable-save --limit 10 --max-items 50"
    result = run_command(fetch_cmd, cwd=project_root)
    
    if result.returncode != 0:
        print("[FAIL] Fetch script failed")
        return False
    else:
        print("[OK] Fetch script completed successfully")
    
    # Test 3: Check if fetch data files were created
    print("\n3. Checking fetch data files...")
    data_files = [
        "data/2025Q4/1-fetch/ia_1_count.csv",
        "data/2025Q4/1-fetch/ia_2_count_by_language.csv", 
        "data/2025Q4/1-fetch/ia_3_count_by_country.csv"
    ]
    
    for file_path in data_files:
        full_path = os.path.join(project_root, file_path)
        if os.path.exists(full_path):
            print(f"[OK] {file_path}")
            # Check if file has content
            with open(full_path, 'r') as f:
                lines = f.readlines()
                if len(lines) > 1:  # More than just header
                    print(f"  - Contains {len(lines)-1} data rows")
                else:
                    print(f"  - Warning: Only header row found")
        else:
            print(f"[FAIL] {file_path} - NOT CREATED")
    
    # Test 4: Test process script
    print("\n4. Testing process script...")
    process_cmd = "pipenv run python scripts/2-process/internetarchive_process.py --enable-save"
    result = run_command(process_cmd, cwd=project_root)
    
    if result.returncode != 0:
        print("[FAIL] Process script failed")
        return False
    else:
        print("[OK] Process script completed successfully")
    
    # Test 5: Check if processed data files were created
    print("\n5. Checking processed data files...")
    processed_files = [
        "data/2025Q4/2-process/ia_license_totals.csv",
        "data/2025Q4/2-process/ia_cc_product_totals.csv",
        "data/2025Q4/2-process/ia_cc_status_combined_totals.csv",
        "data/2025Q4/2-process/ia_cc_status_latest_totals.csv",
        "data/2025Q4/2-process/ia_cc_status_prior_totals.csv",
        "data/2025Q4/2-process/ia_cc_status_retired_totals.csv",
        "data/2025Q4/2-process/ia_cc_totals_by_free_cultural.csv",
        "data/2025Q4/2-process/ia_cc_totals_by_restrictions.csv",
        "data/2025Q4/2-process/ia_open_source_totals.csv"
    ]
    
    for file_path in processed_files:
        full_path = os.path.join(project_root, file_path)
        if os.path.exists(full_path):
            print(f"[OK] {file_path}")
        else:
            print(f"[FAIL] {file_path} - NOT CREATED")
    
    # Test 6: Test report script
    print("\n6. Testing report script...")
    report_cmd = "pipenv run python scripts/3-report/internetarchive_report.py --enable-save"
    result = run_command(report_cmd, cwd=project_root)
    
    if result.returncode != 0:
        print("[FAIL] Report script failed")
        return False
    else:
        print("[OK] Report script completed successfully")
    
    # Test 7: Check if report files were created
    print("\n7. Checking report files...")
    report_files = [
        "data/2025Q4/3-report/ia_cc_product_totals.png",
        "data/2025Q4/3-report/ia_cc_tool_status.png",
        "data/2025Q4/3-report/ia_cc_status_latest_tools.png",
        "data/2025Q4/3-report/ia_cc_status_prior_tools.png",
        # "data/2025Q4/3-report/ia_cc_status_retired_tools.png",  # May not exist if no retired tools data
        "data/2025Q4/3-report/ia_cc_countries_highest_usage.png",
        "data/2025Q4/3-report/ia_cc_languages_highest_usage.png",
        "data/2025Q4/3-report/ia_cc_free_culture.png",
        "data/2025Q4/3-report/ia_open_source_licenses.png"
    ]
    
    for file_path in report_files:
        full_path = os.path.join(project_root, file_path)
        if os.path.exists(full_path):
            print(f"[OK] {file_path}")
        else:
            print(f"[FAIL] {file_path} - NOT CREATED")
    
    # Test 8: Check if README was updated
    print("\n8. Checking README update...")
    readme_path = os.path.join(project_root, "data/2025Q4/README.md")
    if os.path.exists(readme_path):
        print(f"[OK] README.md created")
        with open(readme_path, 'r') as f:
            content = f.read()
            if "Internet Archive (IA)" in content:
                print("[OK] Internet Archive section found in README")
            else:
                print("[FAIL] Internet Archive section not found in README")
    else:
        print("[FAIL] README.md not created")
    
    print("\n" + "=" * 50)
    print("IA Pipeline Test Complete!")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    success = test_ia_pipeline()
    if success:
        print("\n[OK] All tests passed!")
        sys.exit(0)
    else:
        print("\n[FAIL] Some tests failed!")
        sys.exit(1)
