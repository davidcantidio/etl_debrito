#!/usr/bin/env python3
"""
🎯 GitHub Projects v2 Setup Guide - ETL Debrito
===============================================

Script de demonstração e guia para configurar GitHub Projects v2
com custom fields para TDD tracking dos épicos.

IMPORTANTE: Este script requer autenticação manual no GitHub CLI.
Execute: gh auth refresh -s project,read:project,write:project

Uso:
    python setup_projects_v2.py --demo      # Mostra exemplos de configuração
    python setup_projects_v2.py --create    # Tenta criar Projects v2 (requires auth)
    python setup_projects_v2.py --connect   # Conecta Issues existentes
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


class ProjectsV2Setup:
    """Setup para GitHub Projects v2 com TDD fields."""
    
    def __init__(self, repo: str = "davidcantidio/etl_debrito"):
        self.repo = repo
        self.project_title = "ETL Debrito - TDD Epic Development"
        
        # Custom fields configuration
        self.custom_fields = {
            "epic_id": {
                "name": "Epic ID", 
                "type": "single_select",
                "options": ["0", "0.5", "2", "3", "4", "5", "6", "7", "8"]
            },
            "tdd_phase": {
                "name": "TDD Phase",
                "type": "single_select", 
                "options": ["📋 Analysis", "🔴 Red", "🟢 Green", "🟨 Refactor", "✅ Done"]
            },
            "estimated_days": {
                "name": "Estimated Days",
                "type": "number"
            },
            "actual_days": {
                "name": "Actual Days", 
                "type": "number"
            },
            "priority": {
                "name": "Priority",
                "type": "single_select",
                "options": ["🔥 Critical", "📈 High", "📊 Medium", "📋 Low"]
            },
            "epic_status": {
                "name": "Epic Status",
                "type": "single_select", 
                "options": ["⏸️ Not Started", "🔄 Active", "👀 Review", "✅ Completed"]
            },
            "completion_percent": {
                "name": "Completion %",
                "type": "number"
            },
            "dependencies": {
                "name": "Dependencies", 
                "type": "text"
            }
        }
        
        # Views configuration
        self.views = {
            "kanban": {
                "name": "📋 TDD Kanban",
                "layout": "board",
                "group_by": "TDD Phase",
                "description": "Kanban board organized by TDD phases"
            },
            "roadmap": {
                "name": "🗓️ Epic Roadmap",
                "layout": "roadmap", 
                "description": "Timeline view with dependencies"
            },
            "metrics": {
                "name": "📊 Metrics Table",
                "layout": "table",
                "description": "All custom fields for analysis"
            },
            "active": {
                "name": "🎯 Active Epic",
                "layout": "table",
                "filter": "Epic Status:Active",
                "description": "Currently active epic focus view"
            },
            "critical": {
                "name": "🔥 Critical Path", 
                "layout": "table",
                "filter": "Priority:Critical",
                "description": "Critical path epics only"
            }
        }
    
    def show_demo(self):
        """Mostra configuração demonstrativa do Projects v2."""
        print("🎯 GitHub Projects v2 - ETL Debrito TDD Configuration")
        print("=" * 60)
        
        print("\n📋 Project Configuration:")
        print(f"Title: {self.project_title}")
        print(f"Repository: {self.repo}")
        
        print("\n🔧 Custom Fields:")
        for field_id, config in self.custom_fields.items():
            print(f"  • {config['name']} ({config['type']})")
            if config['type'] == 'single_select':
                print(f"    Options: {', '.join(config['options'])}")
        
        print("\n👁️ Views:")
        for view_id, config in self.views.items():
            print(f"  • {config['name']} - {config['layout']}")
            print(f"    {config['description']}")
        
        print("\n🚀 Setup Steps (Manual):")
        print("1. Go to: https://github.com/davidcantidio/etl_debrito")
        print("2. Click 'Projects' tab → 'New project'")
        print("3. Select 'Table' template")
        print(f"4. Name: '{self.project_title}'")
        print("5. Add custom fields using configuration above")
        print("6. Create views using layouts above")
        print("7. Connect existing Issues to project")
        
        print("\n🔗 Automatic Integration:")
        print("• Enhanced commit pattern triggers field updates")
        print("• GitHub Actions workflow updates completion %")
        print("• Issue status changes move cards between columns")
        print("• PR merges update TDD phase automatically")
    
    def generate_gh_commands(self):
        """Gera comandos GitHub CLI para criar Projects v2."""
        print("\n🤖 GitHub CLI Commands:")
        print("=" * 60)
        
        print("# 1. Create project")
        print(f"gh project create --owner @me --title '{self.project_title}'")
        
        print("\n# 2. Add custom fields (requires project ID)")
        print("PROJECT_ID=$(gh project list --owner @me --format json | jq -r '.[] | select(.title==\"ETL Debrito - TDD Epic Development\") | .id')")
        
        for field_id, config in self.custom_fields.items():
            if config['type'] == 'single_select':
                options = ','.join(config['options'])
                print(f"gh project field-create $PROJECT_ID --name '{config['name']}' --single-select-options '{options}'")
            elif config['type'] == 'number':
                print(f"gh project field-create $PROJECT_ID --name '{config['name']}' --number")
            elif config['type'] == 'text':
                print(f"gh project field-create $PROJECT_ID --name '{config['name']}' --text")
        
        print("\n# 3. Add existing issues to project")
        print("gh issue list --repo davidcantidio/etl_debrito --label epic --json number | jq -r '.[].number' | while read issue_num; do")
        print("  gh project item-add $PROJECT_ID --owner @me --content-id $(gh issue view $issue_num --repo davidcantidio/etl_debrito --json id | jq -r '.id')")
        print("done")
        
        print("\n# 4. Set initial field values")
        print("# (This would be done programmatically based on epic IDs)")
    
    def create_project_via_cli(self):
        """Tenta criar projeto via GitHub CLI."""
        try:
            # Check if gh CLI is available and authenticated
            result = subprocess.run(['gh', 'auth', 'status'], 
                                  capture_output=True, text=True)
            
            if result.returncode != 0:
                print("❌ GitHub CLI not authenticated. Run: gh auth login")
                return False
            
            # Create project
            cmd = [
                'gh', 'project', 'create',
                '--owner', '@me', 
                '--title', self.project_title
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            project_url = result.stdout.strip()
            
            print(f"✅ Project created: {project_url}")
            
            # TODO: Add custom fields (requires more complex GraphQL API calls)
            print("⚠️ Custom fields need to be added manually via web interface")
            print("   Use the configuration shown in --demo mode")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error creating project: {e.stderr}")
            return False
        except FileNotFoundError:
            print("❌ GitHub CLI not found. Install from: https://cli.github.com/")
            return False
    
    def connect_existing_issues(self):
        """Conecta Issues existentes ao Projects v2."""
        try:
            # List epic issues
            cmd = [
                'gh', 'issue', 'list', 
                '--repo', self.repo,
                '--label', 'epic',
                '--json', 'number,title'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            issues = json.loads(result.stdout)
            
            if not issues:
                print("⚠️ No issues with 'epic' label found")
                return False
            
            print(f"📋 Found {len(issues)} epic issues:")
            for issue in issues:
                print(f"  • #{issue['number']}: {issue['title']}")
            
            print("\n💡 To connect these to Projects v2:")
            print("1. Go to your Projects v2 board")
            print("2. Click '+ Add item'") 
            print("3. Select 'Add item from repository'")
            print("4. Add each epic issue listed above")
            print("5. Set Epic ID field based on issue title")
            
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error listing issues: {e.stderr}")
            return False
    
    def generate_automation_script(self):
        """Gera script de automação para integração com commits."""
        automation_script = '''#!/usr/bin/env python3
"""
🤖 Projects v2 Automation - TDD Commit Integration
================================================

Script para atualizar Projects v2 baseado em commits TDD enhanced.
Integra com GitHub Actions workflow.
"""

import re
import subprocess
import json
from typing import Dict, List

def parse_tdd_commits() -> Dict[str, Dict]:
    """Parse commits TDD e retorna dados por épico."""
    
    # Pattern para commits TDD enhanced
    pattern = r'\\[EPIC-(\\d+\\.?\\d*)\\]\\s+(analysis|red|green|refactor):\\s+(\\w+):\\s+(.*?)(?:\\s*\\[Task\\s+([\\w.-]+)\\s*\\|\\s*(\\d+)min\\])?'
    
    cmd = 'git log --oneline --grep="\\[EPIC-" --since="30 days ago"'
    commits = subprocess.check_output(cmd, shell=True, text=True).strip()
    
    epic_data = {}
    
    for commit_line in commits.split('\\n'):
        if not commit_line:
            continue
            
        match = re.search(pattern, commit_line)
        if match:
            epic_id, tdd_phase, conv_type, description, task_id, time_minutes = match.groups()
            
            if epic_id not in epic_data:
                epic_data[epic_id] = {
                    'phases': {'analysis': 0, 'red': 0, 'green': 0, 'refactor': 0},
                    'total_time': 0,
                    'current_phase': 'analysis',
                    'completion': 0
                }
            
            epic_data[epic_id]['phases'][tdd_phase] += 1
            epic_data[epic_id]['current_phase'] = tdd_phase
            
            if time_minutes:
                epic_data[epic_id]['total_time'] += int(time_minutes)
            
            # Calculate completion based on highest phase
            phase_weights = {'analysis': 10, 'red': 30, 'green': 70, 'refactor': 100}
            epic_data[epic_id]['completion'] = phase_weights.get(tdd_phase, 0)
    
    return epic_data

def update_projects_v2(epic_data: Dict[str, Dict]):
    """Update Projects v2 fields via GitHub API."""
    
    # This would use GitHub GraphQL API to update custom fields
    # Implementation requires project ID and field IDs from Projects v2
    
    for epic_id, data in epic_data.items():
        print(f"Epic {epic_id}: {data['current_phase']} phase, {data['completion']}% complete")
        
        # TODO: Implement GraphQL mutations to update:
        # - TDD Phase field
        # - Completion % field  
        # - Actual Days field (calculated from total_time)

if __name__ == "__main__":
    epic_data = parse_tdd_commits()
    update_projects_v2(epic_data)
'''
        
        with open('projects_v2_automation.py', 'w') as f:
            f.write(automation_script)
        
        print("✅ Generated: projects_v2_automation.py")
        print("💡 Integrate this with GitHub Actions for automatic updates")


def main():
    parser = argparse.ArgumentParser(description='GitHub Projects v2 setup for ETL Debrito')
    parser.add_argument('--demo', action='store_true', help='Show configuration demo')
    parser.add_argument('--create', action='store_true', help='Create Projects v2 via CLI')
    parser.add_argument('--connect', action='store_true', help='Connect existing issues')
    parser.add_argument('--commands', action='store_true', help='Generate CLI commands')
    parser.add_argument('--automation', action='store_true', help='Generate automation script')
    parser.add_argument('--all', action='store_true', help='Run all setup steps')
    
    args = parser.parse_args()
    
    setup = ProjectsV2Setup()
    
    if args.all or args.demo:
        setup.show_demo()
    
    if args.all or args.commands:
        setup.generate_gh_commands()
    
    if args.all or args.create:
        setup.create_project_via_cli()
    
    if args.all or args.connect:
        setup.connect_existing_issues()
    
    if args.all or args.automation:
        setup.generate_automation_script()
    
    if not any([args.demo, args.create, args.connect, args.commands, args.automation, args.all]):
        parser.print_help()


if __name__ == '__main__':
    main()