#!/usr/bin/env python3
"""
🎯 ETL Debrito - Hierarchical Gantt Generator
============================================

Sistema de visualização hierárquico para 237 tasks granulares:
- Level 1: Epic Overview (9 épicos)
- Level 2: Epic Detail (tasks individuais)
- Level 3: TDD Phase grouping 
- Level 4: Critical Path view

Gera múltiplos arquivos Mermaid para navegação drill-down.

Uso:
    python generate_hierarchical_diagrams.py --all
    python generate_hierarchical_diagrams.py --overview
    python generate_hierarchical_diagrams.py --epics
    python generate_hierarchical_diagrams.py --phases
"""

import argparse
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import re


class HierarchicalGanttGenerator:
    """Gerador de Gantts hierárquicos para 237 tasks."""
    
    def __init__(self, output_dir: str = "docs/visualizations"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        self.epics_data = self.load_all_epics()
        self.all_tasks = self.extract_all_tasks()
        
        # Base date for scheduling
        self.start_date = datetime.now().date()
        
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
            except Exception as e:
                print(f"❌ Erro ao carregar {epic_file}: {e}")
        
        return epics
    
    def extract_all_tasks(self) -> List[Dict]:
        """Extrai todas as tasks com enriquecimento."""
        all_tasks = []
        
        for epic_id, epic_data in self.epics_data.items():
            tasks = epic_data.get('tasks', [])
            for task in tasks:
                enriched_task = {
                    **task,
                    'epic_id': epic_id,
                    'epic_name': epic_data.get('name', 'Unknown Epic'),
                    'full_id': f"{epic_id}.{task.get('id', 'unknown')}"
                }
                all_tasks.append(enriched_task)
        
        return all_tasks
    
    def calculate_epic_duration(self, epic_id: str) -> int:
        """Calcula duração total do épico em minutos."""
        epic_tasks = [t for t in self.all_tasks if t['epic_id'] == epic_id]
        return sum(task.get('estimate_minutes', 0) for task in epic_tasks)
    
    def get_epic_tdd_phases(self, epic_id: str) -> Dict[str, int]:
        """Calcula distribuição de fases TDD para um épico."""
        epic_tasks = [t for t in self.all_tasks if t['epic_id'] == epic_id]
        phases = {'analysis': 0, 'red': 0, 'green': 0, 'refactor': 0}
        
        for task in epic_tasks:
            phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
            phase_normalized = phase.lower().replace('/documentation', '').replace('analysis/documentation task', 'analysis')
            
            if phase_normalized in phases:
                phases[phase_normalized] += 1
            else:
                phases['analysis'] += 1  # Default fallback
        
        return phases
    
    def generate_epic_overview(self) -> str:
        """Gera Gantt de overview dos 9 épicos."""
        
        gantt_lines = [
            "gantt",
            "    title ETL Debrito - Epic Overview",
            "    dateFormat YYYY-MM-DD",
            "    axisFormat %m/%d",
            ""
        ]
        
        current_date = self.start_date
        epic_list = sorted(self.epics_data.keys(), key=lambda x: float(x) if '.' in x else int(x))
        
        for epic_id in epic_list:
            epic = self.epics_data[epic_id]
            duration_minutes = self.calculate_epic_duration(epic_id)
            duration_days = max(1, math.ceil(duration_minutes / 480))  # 8h workday
            
            # Epic status based on TDD phases
            phases = self.get_epic_tdd_phases(epic_id)
            status = self.determine_epic_status(phases)
            
            # Format epic name for Gantt
            epic_name = epic['name'][:40] + "..." if len(epic['name']) > 40 else epic['name']
            
            end_date = current_date + timedelta(days=duration_days)
            
            gantt_lines.append(
                f"    section Epic {epic_id}"
            )
            gantt_lines.append(
                f"    {epic_name}    :{status}, epic-{epic_id}, {current_date.isoformat()}, {duration_days}d"
            )
            gantt_lines.append("")
            
            current_date = end_date + timedelta(days=1)  # Gap between epics
        
        return "\n".join(gantt_lines)
    
    def determine_epic_status(self, phases: Dict[str, int]) -> str:
        """Determina status do épico baseado nas fases TDD."""
        total = sum(phases.values())
        if total == 0:
            return "milestone"
        
        completion_ratio = (phases['green'] + phases['refactor']) / total
        
        if completion_ratio >= 0.8:
            return "done"
        elif completion_ratio >= 0.4:
            return "active"
        elif phases['red'] > phases['analysis']:
            return "crit"
        else:
            return ""  # Default/pending
    
    def generate_epic_detail(self, epic_id: str) -> str:
        """Gera Gantt detalhado para um épico específico."""
        
        epic = self.epics_data[epic_id]
        epic_tasks = [t for t in self.all_tasks if t['epic_id'] == epic_id]
        
        gantt_lines = [
            "gantt",
            f"    title Epic {epic_id}: {epic['name']}",
            "    dateFormat YYYY-MM-DD",
            "    axisFormat %m/%d",
            ""
        ]
        
        # Group tasks by TDD phase for better organization
        tasks_by_phase = {
            'analysis': [],
            'red': [],
            'green': [],
            'refactor': []
        }
        
        for task in epic_tasks:
            phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
            phase_normalized = phase.lower().replace('/documentation', '').replace('analysis/documentation task', 'analysis')
            
            if phase_normalized in tasks_by_phase:
                tasks_by_phase[phase_normalized].append(task)
            else:
                tasks_by_phase['analysis'].append(task)
        
        current_date = self.start_date
        
        # Generate sections by TDD phase
        for phase_name, phase_tasks in tasks_by_phase.items():
            if not phase_tasks:
                continue
                
            gantt_lines.append(f"    section {phase_name.title()} Phase")
            
            for task in sorted(phase_tasks, key=lambda x: x.get('id', '')):
                task_name = task.get('title', 'Untitled Task')[:50]
                task_name = task_name.replace("TEST: ", "").replace("IMPL: ", "").replace("REFACTOR: ", "")
                
                duration_minutes = task.get('estimate_minutes', 10)
                duration_days = max(1, math.ceil(duration_minutes / 480))
                
                # Task status
                status = self.get_task_status(task)
                
                end_date = current_date + timedelta(days=duration_days)
                
                gantt_lines.append(
                    f"    {task_name}    :{status}, task-{task['id']}, {current_date.isoformat()}, {duration_days}d"
                )
                
                current_date = end_date
            
            gantt_lines.append("")
        
        return "\n".join(gantt_lines)
    
    def get_task_status(self, task: Dict) -> str:
        """Determina status visual da task no Gantt."""
        phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
        
        if 'red' in phase.lower():
            return "crit"  # Red tasks are critical/urgent
        elif 'green' in phase.lower():
            return "active"  # Green tasks are in progress
        elif 'refactor' in phase.lower():
            return "done"  # Refactor is near completion
        else:
            return ""  # Analysis/planning phase
    
    def generate_tdd_phase_view(self, phase: str) -> str:
        """Gera Gantt agrupado por fase TDD específica."""
        
        phase_tasks = []
        for task in self.all_tasks:
            task_phase = task.get('tdd_phase', task.get('tdd_skip_reason', 'analysis'))
            task_phase_normalized = task_phase.lower().replace('/documentation', '').replace('analysis/documentation task', 'analysis')
            
            if phase.lower() in task_phase_normalized:
                phase_tasks.append(task)
        
        if not phase_tasks:
            return f"gantt\n    title {phase.title()} Phase - No Tasks Found"
        
        gantt_lines = [
            "gantt",
            f"    title {phase.title()} Phase Tasks ({len(phase_tasks)} total)",
            "    dateFormat YYYY-MM-DD",
            "    axisFormat %m/%d",
            ""
        ]
        
        # Group by epic for organization
        tasks_by_epic = {}
        for task in phase_tasks:
            epic_id = task['epic_id']
            if epic_id not in tasks_by_epic:
                tasks_by_epic[epic_id] = []
            tasks_by_epic[epic_id].append(task)
        
        current_date = self.start_date
        
        for epic_id in sorted(tasks_by_epic.keys(), key=lambda x: float(x) if '.' in x else int(x)):
            epic_tasks = tasks_by_epic[epic_id]
            epic_name = self.epics_data[epic_id]['name'][:30]
            
            gantt_lines.append(f"    section Epic {epic_id}: {epic_name}")
            
            for task in sorted(epic_tasks, key=lambda x: x.get('id', '')):
                task_name = task.get('title', 'Untitled')[:50]
                task_name = task_name.replace("TEST: ", "").replace("IMPL: ", "").replace("REFACTOR: ", "")
                
                duration_minutes = task.get('estimate_minutes', 10)
                duration_days = max(1, math.ceil(duration_minutes / 480))
                
                status = "active" if phase == "green" else ("crit" if phase == "red" else "done" if phase == "refactor" else "")
                
                end_date = current_date + timedelta(days=duration_days)
                
                gantt_lines.append(
                    f"    {task_name}    :{status}, {task['id']}, {current_date.isoformat()}, {duration_days}d"
                )
                
                current_date = end_date
            
            gantt_lines.append("")
        
        return "\n".join(gantt_lines)
    
    def identify_critical_path(self) -> List[Dict]:
        """Identifica tasks no critical path baseado em dependencies."""
        # Simplified critical path: highest priority tasks with dependencies
        critical_tasks = []
        
        for task in self.all_tasks:
            # Factors for critical path:
            # 1. Has dependencies (blocks other tasks)
            # 2. High story points (complex/important)
            # 3. Long duration (bottleneck potential)
            
            dependencies = task.get('dependencies', [])
            story_points = task.get('story_points', 0)
            duration = task.get('estimate_minutes', 0)
            
            # Critical score calculation
            critical_score = 0
            
            if dependencies:  # Has dependencies
                critical_score += len(dependencies) * 10
            
            if story_points >= 15:  # High complexity
                critical_score += 20
            
            if duration >= 60:  # Long duration (1+ hour)
                critical_score += 15
            
            # TDD phase influence
            phase = task.get('tdd_phase', '')
            if 'red' in phase.lower():  # Tests block implementation
                critical_score += 25
            
            if critical_score >= 20:  # Threshold for critical path
                task['critical_score'] = critical_score
                critical_tasks.append(task)
        
        # Sort by critical score
        return sorted(critical_tasks, key=lambda x: x['critical_score'], reverse=True)
    
    def generate_critical_path_view(self) -> str:
        """Gera Gantt do critical path."""
        
        critical_tasks = self.identify_critical_path()[:30]  # Top 30 critical tasks
        
        gantt_lines = [
            "gantt",
            f"    title Critical Path ({len(critical_tasks)} tasks)",
            "    dateFormat YYYY-MM-DD",
            "    axisFormat %m/%d",
            ""
        ]
        
        if not critical_tasks:
            gantt_lines.append("    section No Critical Tasks")
            gantt_lines.append("    Analysis Required    :milestone, m1, 2025-01-10, 0d")
            return "\n".join(gantt_lines)
        
        current_date = self.start_date
        
        gantt_lines.append("    section Critical Path")
        
        for task in critical_tasks:
            task_name = task.get('title', 'Critical Task')[:40]
            epic_id = task['epic_id']
            
            duration_minutes = task.get('estimate_minutes', 10)
            duration_days = max(1, math.ceil(duration_minutes / 480))
            
            end_date = current_date + timedelta(days=duration_days)
            
            gantt_lines.append(
                f"    [E{epic_id}] {task_name}    :crit, {task['id']}, {current_date.isoformat()}, {duration_days}d"
            )
            
            current_date = end_date
        
        return "\n".join(gantt_lines)
    
    def generate_all_diagrams(self) -> None:
        """Gera todos os diagramas hierárquicos."""
        
        print("🎯 Generating hierarchical Gantt diagrams...")
        
        # 1. Epic Overview
        overview_content = self.generate_epic_overview()
        overview_path = self.output_dir / "overview" / "gantt_epics.mmd"
        overview_path.parent.mkdir(parents=True, exist_ok=True)
        overview_path.write_text(overview_content, encoding='utf-8')
        print(f"✅ Epic Overview: {overview_path}")
        
        # 2. Individual Epic Details
        epics_dir = self.output_dir / "epics"
        epics_dir.mkdir(parents=True, exist_ok=True)
        
        for epic_id in self.epics_data.keys():
            epic_content = self.generate_epic_detail(epic_id)
            epic_path = epics_dir / f"gantt_epic_{epic_id}.mmd"
            epic_path.write_text(epic_content, encoding='utf-8')
            print(f"✅ Epic {epic_id}: {epic_path}")
        
        # 3. TDD Phase Views
        phases_dir = self.output_dir / "phases"
        phases_dir.mkdir(parents=True, exist_ok=True)
        
        for phase in ['analysis', 'red', 'green', 'refactor']:
            phase_content = self.generate_tdd_phase_view(phase)
            phase_path = phases_dir / f"gantt_{phase}.mmd"
            phase_path.write_text(phase_content, encoding='utf-8')
            print(f"✅ TDD {phase.title()}: {phase_path}")
        
        # 4. Critical Path
        critical_content = self.generate_critical_path_view()
        critical_path = self.output_dir / "overview" / "gantt_critical_path.mmd"
        critical_path.write_text(critical_content, encoding='utf-8')
        print(f"✅ Critical Path: {critical_path}")
        
        print(f"\n🎉 All diagrams generated in: {self.output_dir}")
        print("📊 Structure:")
        print(f"  📁 overview/    - Epic overview & critical path")
        print(f"  📁 epics/       - {len(self.epics_data)} detailed epic Gantts")
        print(f"  📁 phases/      - 4 TDD phase groupings")


def main():
    parser = argparse.ArgumentParser(description='Generate hierarchical Gantt diagrams')
    parser.add_argument('--all', action='store_true', help='Generate all diagram types')
    parser.add_argument('--overview', action='store_true', help='Generate epic overview only')
    parser.add_argument('--epics', action='store_true', help='Generate individual epic diagrams')
    parser.add_argument('--phases', action='store_true', help='Generate TDD phase groupings')
    parser.add_argument('--critical', action='store_true', help='Generate critical path only')
    parser.add_argument('--output', default='docs/visualizations', help='Output directory')
    
    args = parser.parse_args()
    
    generator = HierarchicalGanttGenerator(args.output)
    
    if args.all or (not any([args.overview, args.epics, args.phases, args.critical])):
        generator.generate_all_diagrams()
    else:
        if args.overview:
            overview = generator.generate_epic_overview()
            overview_path = Path(args.output) / "overview" / "gantt_epics.mmd"
            overview_path.parent.mkdir(parents=True, exist_ok=True)
            overview_path.write_text(overview, encoding='utf-8')
            print(f"✅ Epic Overview: {overview_path}")
        
        if args.epics:
            epics_dir = Path(args.output) / "epics"
            epics_dir.mkdir(parents=True, exist_ok=True)
            for epic_id in generator.epics_data.keys():
                content = generator.generate_epic_detail(epic_id)
                path = epics_dir / f"gantt_epic_{epic_id}.mmd"
                path.write_text(content, encoding='utf-8')
                print(f"✅ Epic {epic_id}: {path}")
        
        if args.phases:
            phases_dir = Path(args.output) / "phases"
            phases_dir.mkdir(parents=True, exist_ok=True)
            for phase in ['analysis', 'red', 'green', 'refactor']:
                content = generator.generate_tdd_phase_view(phase)
                path = phases_dir / f"gantt_{phase}.mmd"
                path.write_text(content, encoding='utf-8')
                print(f"✅ TDD {phase.title()}: {path}")
        
        if args.critical:
            critical = generator.generate_critical_path_view()
            critical_path = Path(args.output) / "overview" / "gantt_critical_path.mmd"
            critical_path.parent.mkdir(parents=True, exist_ok=True)
            critical_path.write_text(critical, encoding='utf-8')
            print(f"✅ Critical Path: {critical_path}")


if __name__ == '__main__':
    main()