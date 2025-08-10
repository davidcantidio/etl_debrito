#!/usr/bin/env python3
"""
Setup GitHub Labels for ETL Debrito Project

Creates all necessary labels for the 237 granular issues system:
- Epic labels (epic-0, epic-0.5, etc.)
- TDD Phase labels (tdd-analysis, tdd-red, etc.)
- Estimate labels (estimate-small, estimate-medium, estimate-large)
- Type labels (type-test, type-implementation, etc.)

Usage:
    python3 setup_github_labels.py [--repo OWNER/REPO] [--dry-run]
"""

import subprocess
import sys
import json
from typing import List, Dict, Tuple
from pathlib import Path
import argparse


class GitHubLabelsSetup:
    def __init__(self, repo: str = None, dry_run: bool = False):
        self.repo = repo or self._detect_repo()
        self.dry_run = dry_run
        self.labels_created = []
        self.labels_updated = []
        self.labels_skipped = []
        
    def _detect_repo(self) -> str:
        """Auto-detect GitHub repository from git remote."""
        try:
            result = subprocess.run(['gh', 'repo', 'view', '--json', 'nameWithOwner'], 
                                  capture_output=True, text=True, check=True)
            repo_data = json.loads(result.stdout)
            return repo_data['nameWithOwner']
        except Exception:
            # Fallback: try git remote
            try:
                result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                                      capture_output=True, text=True, check=True)
                url = result.stdout.strip()
                if 'github.com' in url:
                    # Extract owner/repo from URL
                    if url.startswith('git@github.com:'):
                        repo = url.replace('git@github.com:', '').replace('.git', '')
                    elif 'github.com/' in url:
                        repo = url.split('github.com/')[-1].replace('.git', '')
                    else:
                        raise ValueError("Cannot parse GitHub URL")
                    return repo
            except Exception as e:
                raise ValueError(f"Cannot detect repository. Please specify with --repo. Error: {e}")

    def get_label_definitions(self) -> List[Tuple[str, str, str]]:
        """Get all label definitions as (name, color, description) tuples."""
        labels = []
        
        # Epic Labels - Blue gradient
        epic_colors = {
            '0': '1f77b4',      # Deep blue
            '0.5': '2e86d4',    # Medium blue  
            '2': '3d95e4',      # Light blue
            '3': '4ca4f4',      # Lighter blue
            '4': '5bb3ff',      # Very light blue
            '5': '6ac2ff',      # Cyan blue
            '6': '79d1ff',      # Light cyan
            '7': '88e0ff',      # Very light cyan
            '8': '97efff'       # Pale cyan
        }
        
        for epic_id in ['0', '0.5', '2', '3', '4', '5', '6', '7', '8']:
            labels.append((
                f"epic-{epic_id}",
                epic_colors[epic_id],
                f"Tasks belonging to Epic {epic_id}"
            ))
        
        # TDD Phase Labels
        tdd_labels = [
            ('tdd-analysis', '6f42c1', '🔍 Analysis phase - research and planning'),
            ('tdd-red', 'e74c3c', '🔴 Red phase - failing test first'),
            ('tdd-green', '27ae60', '🟢 Green phase - minimal implementation'),
            ('tdd-refactor', 'f39c12', '🟡 Refactor phase - improve and optimize')
        ]
        labels.extend(tdd_labels)
        
        # Estimate Labels
        estimate_labels = [
            ('estimate-small', 'd4edda', '⚡ Small task (≤15 min)'),
            ('estimate-medium', 'fff3cd', '⏳ Medium task (16-60 min)'), 
            ('estimate-large', 'f8d7da', '⏰ Large task (>60 min)')
        ]
        labels.extend(estimate_labels)
        
        # Type Labels
        type_labels = [
            ('type-test', 'ff6b9d', '🧪 Test development and validation'),
            ('type-implementation', '4ecdc4', '⚙️ Feature implementation'),
            ('type-refactor', 'ffc048', '🔄 Code refactoring and improvement'),
            ('type-analysis', '9b59b6', '📊 Analysis and documentation')
        ]
        labels.extend(type_labels)
        
        return labels

    def label_exists(self, label_name: str) -> bool:
        """Check if label already exists in repository."""
        try:
            cmd = ['gh', 'api', f'repos/{self.repo}/labels/{label_name}']
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            return result.returncode == 0
        except Exception:
            return False

    def create_label(self, name: str, color: str, description: str) -> bool:
        """Create a single label."""
        if self.dry_run:
            print(f"🔄 DRY RUN - Would create label: {name} (#{color}) - {description}")
            return True
            
        try:
            # Check if exists first
            if self.label_exists(name):
                self.labels_skipped.append(name)
                print(f"⏭️  Label '{name}' already exists, skipping")
                return True
                
            # Create label
            cmd = [
                'gh', 'api', f'repos/{self.repo}/labels',
                '--method', 'POST',
                '--field', f'name={name}',
                '--field', f'color={color}',
                '--field', f'description={description}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.labels_created.append(name)
            print(f"✅ Created label: {name}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to create label '{name}': {e.stderr}")
            return False
        except Exception as e:
            print(f"❌ Error creating label '{name}': {e}")
            return False

    def setup_all_labels(self) -> bool:
        """Create all required labels."""
        print(f"🚀 Setting up GitHub labels for repository: {self.repo}")
        print(f"{'📋 DRY RUN MODE - No actual changes will be made' if self.dry_run else '🔧 LIVE MODE - Labels will be created'}")
        print()
        
        labels = self.get_label_definitions()
        success_count = 0
        
        for name, color, description in labels:
            if self.create_label(name, color, description):
                success_count += 1
                
        # Summary
        print(f"\n🎯 Setup Complete!")
        print(f"✅ Successfully processed: {success_count}/{len(labels)} labels")
        
        if not self.dry_run:
            print(f"🆕 Created: {len(self.labels_created)} labels")
            print(f"⏭️  Skipped (already exist): {len(self.labels_skipped)} labels")
            
            if self.labels_created:
                print(f"📝 Created labels: {', '.join(self.labels_created)}")
                
        return success_count == len(labels)

    def verify_gh_cli(self) -> bool:
        """Verify GitHub CLI is installed and authenticated."""
        try:
            # Check if gh is installed
            result = subprocess.run(['gh', '--version'], capture_output=True, text=True, check=True)
            print(f"✅ GitHub CLI detected: {result.stdout.split()[2]}")
            
            # Check authentication
            result = subprocess.run(['gh', 'auth', 'status'], capture_output=True, text=True, check=True)
            print(f"✅ GitHub CLI authenticated")
            return True
            
        except FileNotFoundError:
            print("❌ GitHub CLI (gh) not found. Install it first:")
            print("   https://cli.github.com/")
            return False
        except subprocess.CalledProcessError:
            print("❌ GitHub CLI not authenticated. Run: gh auth login")
            return False


def main():
    parser = argparse.ArgumentParser(description='Setup GitHub labels for ETL Debrito project')
    parser.add_argument('--repo', help='GitHub repository (owner/repo)', default=None)
    parser.add_argument('--dry-run', action='store_true', help='Show what would be created without making changes')
    
    args = parser.parse_args()
    
    try:
        setup = GitHubLabelsSetup(repo=args.repo, dry_run=args.dry_run)
        
        # Verify prerequisites
        if not setup.verify_gh_cli():
            sys.exit(1)
            
        print(f"🎯 Repository: {setup.repo}")
        print()
        
        # Setup labels
        success = setup.setup_all_labels()
        
        if success:
            print(f"\n🎉 All labels are ready!")
            if not args.dry_run:
                print(f"➡️  You can now run: python3 create_granular_issues.py --create")
        else:
            print(f"\n⚠️  Some labels failed to create. Check the output above.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()