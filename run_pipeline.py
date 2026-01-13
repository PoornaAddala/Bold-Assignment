#!/usr/bin/env python3
"""
Quick Run Script for Healthcare Eligibility Pipeline
=====================================================
This script provides a convenient way to run the pipeline with default settings.
"""

import subprocess
import sys
from pathlib import Path


def main():
    """Run the pipeline with default settings."""
    # Get the project root directory
    project_root = Path(__file__).parent
    
    # Define paths
    config_path = project_root / "config" / "partners.yaml"
    input_dir = project_root / "data" / "input"
    output_path = project_root / "data" / "output" / "unified_eligibility.csv"
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build command
    cmd = [
        sys.executable,
        str(project_root / "src" / "pipeline.py"),
        "--config", str(config_path),
        "--input", str(input_dir),
        "--output", str(output_path),
    ]
    
    # Add any additional arguments passed to this script
    cmd.extend(sys.argv[1:])
    
    # Run pipeline
    print("=" * 60)
    print("Running Healthcare Eligibility Pipeline")
    print("=" * 60)
    print(f"Config:  {config_path}")
    print(f"Input:   {input_dir}")
    print(f"Output:  {output_path}")
    print("=" * 60)
    print()
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print()
        print("=" * 60)
        print("Pipeline completed successfully!")
        print(f"Output file: {output_path}")
        print("=" * 60)
        
        # Display output preview
        if output_path.exists():
            print()
            print("Output Preview:")
            print("-" * 60)
            with open(output_path, "r") as f:
                for i, line in enumerate(f):
                    print(line.rstrip())
                    if i >= 5:  # Show header + first 5 data rows
                        break
    
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
