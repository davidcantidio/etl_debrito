#!/usr/bin/env python3
"""
🎯 ETL Debrito - Granular Issues Creator
=======================================

Script para criar 237 issues granulares baseadas nas tasks dos épicos JSON.
Baseado na estrutura funcional do setup_generic_issue.sh.

Funcionalidades:
- Parse automático dos 9 épicos JSON (237 total tasks)
- Criação de issues individuais para cada task
- Adição automática ao Projects v2 (#9)
- Population de custom fields (Epic ID, TDD Phase, estimates)
- Rate limiting e error handling

Uso:
    python create_granular_issues.py --dry-run    # Preview apenas
    python create_granular_issues.py --create     # Criar issues
    python create_granular_issues.py --batch 10   # Processar em batches
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re


class GranularIssueCreator:
    """Criador de issues granulares baseado nos épicos JSON."""
    
    def __init__(self, repo: str = "davidcantidio/etl_debrito", 
                 project_number: int = 9, project_owner: str = "davidcantidio"):
        self.repo = repo
        self.project_number = project_number
        self.project_owner = project_owner
        self.dry_run = False
        self.batch_size = 20
        self.rate_limit_delay = 2  # seconds between requests
        
        # Load all epics data
        self.epics_data = self.load_all_epics()
        self.all_tasks = self.extract_all_tasks()
        
        print(f"📊 Loaded {len(self.epics_data)} epics with {len(self.all_tasks)} total tasks")
    
    def load_all_epics(self) -> Dict[str, Dict]:
        """Carrega todos os épicos JSON."""
        epics = {}
        epic_files = list(Path('.').glob('epico_*.json'))
        
        for epic_file in epic_files:
            try:
                with open(epic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    epic_id = str(data['epic']['id'])
                    epics[epic_id] = data['epic']
                    print(f"✅ {epic_file.name}: Epic {epic_id} ({len(data['epic'].get('tasks', []))} tasks)")
            except Exception as e:
                print(f"❌ Erro ao carregar {epic_file}: {e}")
        
        return epics
    
    def extract_all_tasks(self) -> List[Dict]:
        """Extrai todas as tasks de todos os épicos."""
        all_tasks = []
        
        for epic_id, epic_data in self.epics_data.items():
            tasks = epic_data.get('tasks', [])
            for task in tasks:
                # Enrich task with epic context
                enriched_task = {
                    **task,
                    'epic_id': epic_id,
                    'epic_name': epic_data.get('name', 'Unknown Epic'),
                    'epic_labels': epic_data.get('labels', [])
                }
                all_tasks.append(enriched_task)
        
        return all_tasks
    
    def generate_issue_title(self, task: Dict) -> str:
        """Gera título consistente para a issue."""
        epic_id = task['epic_id']
        task_id = task['id']
        title = task['title']
        
        return f"[E{epic_id}T{task_id}] {title}"
    
    def generate_issue_body(self, task: Dict) -> str:
        """Gera corpo detalhado da issue baseado na task."""
        
        # Extract task data
        description = task.get('description', 'No description provided')
        acceptance_criteria = task.get('acceptance_criteria', [])
        deliverables = task.get('deliverables', [])
        dependencies = task.get('dependencies', [])
        estimate_minutes = task.get('estimate_minutes', 0)
        story_points = task.get('story_points', 0)
        tdd_phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
        risk = task.get('risk', '')
        mitigation = task.get('mitigation', '')
        
        # Build checklist from deliverables
        checklist_items = []
        for deliverable in deliverables:
            checklist_items.append(f"- [ ] {deliverable}")
        
        # Build acceptance criteria list
        acceptance_list = []
        for criteria in acceptance_criteria:
            acceptance_list.append(f"- [ ] {criteria}")
        
        # Dependencies section
        dependencies_section = ""
        if dependencies:
            deps_text = ", ".join([f"#{self._get_issue_number_for_task(dep) or f'Task {dep}'}" for dep in dependencies])
            dependencies_section = f"""
### Dependencies
This task depends on: {deps_text}
"""
        
        # Risk section
        risk_section = ""
        if risk:
            risk_section = f"""
### Risk & Mitigation
**Risk**: {risk}
**Mitigation**: {mitigation}
"""
        
        body = f"""### Epic Context
**Epic {task['epic_id']}**: {task['epic_name']}
**Task ID**: {task['id']}
**TDD Phase**: `{tdd_phase}`

### Description
{description}

### Checklist
{chr(10).join(checklist_items) if checklist_items else "- [ ] Complete task implementation"}

### Acceptance Criteria
{chr(10).join(acceptance_list) if acceptance_list else "- [ ] Task completed according to description"}
{dependencies_section}{risk_section}
### Estimates
- **Time**: {estimate_minutes} minutes ({estimate_minutes/60:.1f}h)
- **Story Points**: {story_points}

### Definition of Done
- [ ] All acceptance criteria met
- [ ] Code reviewed (if applicable)
- [ ] Tests passing (if applicable)  
- [ ] Documentation updated (if applicable)
- [ ] Issue moved to Done in Projects

⏱️ **Estimated effort**: {estimate_minutes}min | 📊 **Story points**: {story_points}"""
        
        return body
    
    def generate_issue_labels(self, task: Dict) -> List[str]:
        """Gera labels apropriadas para a issue."""
        labels = []
        
        # Epic label
        labels.append(f"epic-{task['epic_id']}")
        
        # TDD Phase label
        tdd_phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
        if tdd_phase in ['red', 'green', 'refactor', 'analysis']:
            labels.append(f"tdd-{tdd_phase}")
        
        # Estimate bucket label
        estimate = task.get('estimate_minutes', 0)
        if estimate <= 15:
            labels.append("estimate-small")
        elif estimate <= 60:
            labels.append("estimate-medium")
        else:
            labels.append("estimate-large")
        
        # Epic-specific labels
        epic_labels = task.get('epic_labels', [])
        for label in epic_labels[:2]:  # Limit to first 2 epic labels
            if label not in ['tdd', 'epic']:  # Avoid duplicates
                labels.append(label)
        
        # Task type label
        title_lower = task['title'].lower()
        if 'test' in title_lower or tdd_phase == 'red':
            labels.append("type-test")
        elif 'impl' in title_lower or tdd_phase == 'green':
            labels.append("type-implementation")
        elif 'refactor' in title_lower or tdd_phase == 'refactor':
            labels.append("type-refactor")
        elif 'analis' in title_lower or 'documentation' in title_lower:
            labels.append("type-analysis")
        
        return labels[:6]  # GitHub limit: max 10 labels, keeping reasonable
    
    def get_epic_milestone(self, epic_id: str) -> Optional[str]:
        """Obter ou criar milestone para o épico."""
        milestone_title = f"EPIC {epic_id} - {self.epics_data[epic_id]['name']}"
        
        try:
            # Check if milestone exists
            cmd = ['gh', 'api', f'repos/{self.repo}/milestones', '--jq', '.[].title']
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            existing_milestones = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            if milestone_title in existing_milestones:
                return milestone_title
            
            if not self.dry_run:
                # Create milestone
                duration = self.epics_data[epic_id].get('duration', '1 dia')
                create_cmd = [
                    'gh', 'api', '-X', 'POST', f'repos/{self.repo}/milestones',
                    '-f', f'title={milestone_title}',
                    '-f', f'description=Epic {epic_id}: {duration}',
                ]
                subprocess.run(create_cmd, capture_output=True, check=True)
                print(f"✅ Created milestone: {milestone_title}")
            
            return milestone_title
            
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Error handling milestone for Epic {epic_id}: {e}")
            return None
    
    def _get_issue_number_for_task(self, task_id: str) -> Optional[int]:
        """Obtém número da issue para uma task (para dependencies)."""
        # TODO: Implement lookup table after issues are created
        return None
    
    def create_single_issue(self, task: Dict) -> Optional[str]:
        """Cria uma única issue para a task."""
        
        title = self.generate_issue_title(task)
        body = self.generate_issue_body(task)
        labels = self.generate_issue_labels(task)
        milestone = self.get_epic_milestone(task['epic_id'])
        
        if self.dry_run:
            print(f"\n📋 DRY RUN - Would create issue:")
            print(f"Title: {title}")
            print(f"Labels: {', '.join(labels)}")
            print(f"Milestone: {milestone}")
            print(f"Body: {body[:200]}...")
            return f"dry-run-url-{task['id']}"
        
        try:
            # Build gh command
            cmd = [
                'gh', 'issue', 'create', '-R', self.repo,
                '--title', title,
                '--assignee', '@me',
                '--body', body,
                '--label', ','.join(labels)
            ]
            
            if milestone:
                cmd.extend(['--milestone', milestone])
            
            # Create issue
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            issue_url = result.stdout.strip()
            
            # Add to project
            self.add_to_project(issue_url)
            
            print(f"✅ Created: {title}")
            return issue_url
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error creating issue for task {task['id']}: {e.stderr}")
            return None
    
    def add_to_project(self, issue_url: str) -> bool:
        """Adiciona issue ao Projects v2."""
        if self.dry_run:
            return True
            
        try:
            cmd = [
                'gh', 'project', 'item-add', str(self.project_number),
                '--owner', self.project_owner,
                '--url', issue_url
            ]
            subprocess.run(cmd, capture_output=True, check=True)
            return True
        except subprocess.CalledProcessError:
            print(f"⚠️ Failed to add {issue_url} to project")
            return False
    
    def create_all_issues(self) -> None:
        """Cria todas as issues em batches."""
        
        total_tasks = len(self.all_tasks)
        print(f"\n🚀 Starting creation of {total_tasks} granular issues...")
        
        if not self.dry_run:
            # Check auth
            try:
                subprocess.run(['gh', 'auth', 'status'], capture_output=True, check=True)
            except subprocess.CalledProcessError:
                print("❌ GitHub CLI not authenticated. Run: gh auth login")
                return
        
        created_count = 0
        failed_count = 0
        
        # Process in batches
        for i in range(0, total_tasks, self.batch_size):
            batch = self.all_tasks[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_tasks + self.batch_size - 1) // self.batch_size
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} tasks)")
            
            for task in batch:
                issue_url = self.create_single_issue(task)
                if issue_url:
                    created_count += 1
                else:
                    failed_count += 1
                
                # Rate limiting
                if not self.dry_run:
                    time.sleep(self.rate_limit_delay)
            
            print(f"✅ Batch {batch_num} complete: {len([t for t in batch if self.create_single_issue(t)])} created")
        
        print(f"\n🎉 Summary:")
        print(f"  Total tasks: {total_tasks}")
        print(f"  Created: {created_count}")
        print(f"  Failed: {failed_count}")
        print(f"  Success rate: {(created_count/total_tasks)*100:.1f}%")
    
    def show_summary(self) -> None:
        """Mostra resumo das tasks que serão criadas."""
        print(f"\n📊 Task Creation Summary:")
        print(f"  Total tasks: {len(self.all_tasks)}")
        
        # Group by epic
        by_epic = {}
        for task in self.all_tasks:
            epic_id = task['epic_id']
            if epic_id not in by_epic:
                by_epic[epic_id] = []
            by_epic[epic_id].append(task)
        
        print(f"\n📋 Tasks by Epic:")
        for epic_id in sorted(by_epic.keys(), key=lambda x: float(x) if '.' in x else int(x)):
            tasks = by_epic[epic_id]
            epic_name = tasks[0]['epic_name']
            print(f"  Epic {epic_id}: {len(tasks)} tasks - {epic_name}")
        
        # Group by TDD phase
        by_phase = {}
        for task in self.all_tasks:
            phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
            if phase not in by_phase:
                by_phase[phase] = 0
            by_phase[phase] += 1
        
        print(f"\n🧪 Tasks by TDD Phase:")
        for phase, count in sorted(by_phase.items()):
            print(f"  {phase}: {count} tasks")
        
        # Time estimates
        total_minutes = sum(task.get('estimate_minutes', 0) for task in self.all_tasks)
        total_story_points = sum(task.get('story_points', 0) for task in self.all_tasks)
        
        print(f"\n⏱️ Estimates:")
        print(f"  Total time: {total_minutes} minutes ({total_minutes/60:.1f} hours)")
        print(f"  Total story points: {total_story_points}")
        print(f"  Average per task: {total_minutes/len(self.all_tasks):.1f} minutes")


def main():
    parser = argparse.ArgumentParser(description='Create granular GitHub Issues from Epic JSON tasks')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, do not create issues')
    parser.add_argument('--create', action='store_true', help='Actually create the issues')
    parser.add_argument('--summary', action='store_true', help='Show summary of tasks to be created')
    parser.add_argument('--batch', type=int, default=20, help='Batch size for processing (default: 20)')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between requests in seconds (default: 2.0)')
    
    args = parser.parse_args()
    
    creator = GranularIssueCreator()
    creator.dry_run = args.dry_run
    creator.batch_size = args.batch
    creator.rate_limit_delay = args.delay
    
    if args.summary or (not args.create and not args.dry_run):
        creator.show_summary()
    
    if args.create or args.dry_run:
        creator.create_all_issues()


if __name__ == '__main__':
    main()