#!/usr/bin/env python3
"""
🧠 ETL Debrito - Gerador de Diagramas Mermaid TDD com Integração GitHub
======================================================================

Processa todos os arquivos epico_*.json e gera automaticamente:
1. Mindmap hierárquico (épicos → tasks com emojis TDD)  
2. Flowchart de dependências (DAG épicos + tasks críticas)
3. Gantt cronograma profissional com integração GitHub

🔗 GitHub Integration Features:
- Links interativos para Issues por épico
- Status automático baseado em GitHub Issues
- Milestones conectados aos GitHub Milestones
- Task tags profissionais (done, active, crit, milestone)

Uso:
    python generate_all_diagrams.py [--github-repo owner/repo]

Saídas:
    docs/mindmap.mmd
    docs/flow_dependencies.mmd  
    docs/gantt_schedule.mmd
"""

import argparse
import json
import math
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class Task:
    """Representa uma task de um épico TDD com integração GitHub."""
    id: str
    title: str
    tdd_phase: str = "analysis"  # red, green, refactor, analysis
    tdd_skip_reason: Optional[str] = None
    estimate_minutes: int = 10
    dependencies: List[str] = field(default_factory=list)
    deliverables: List[str] = field(default_factory=list)
    global_id: str = ""
    # GitHub Integration
    github_issue_url: Optional[str] = None
    github_status: Optional[str] = None  # done, active, crit, milestone
    is_critical: bool = False
    
    def __post_init__(self):
        """Normaliza dados da task."""
        # Determinar tdd_phase se skip_reason existe
        if self.tdd_skip_reason and not hasattr(self, '_original_tdd_phase'):
            self.tdd_phase = "analysis"
        
        # Garantir estimate_minutes válido
        if not isinstance(self.estimate_minutes, int) or self.estimate_minutes <= 0:
            self.estimate_minutes = 10


@dataclass  
class Epic:
    """Representa um épico TDD completo com integração GitHub."""
    id: str
    name: str
    tasks: List[Task] = field(default_factory=list)
    duration_minutes: int = 0
    duration_days: int = 0
    # GitHub Integration
    github_issue_url: Optional[str] = None
    github_milestone_url: Optional[str] = None
    status: str = "pending"  # done, active, pending
    is_critical: bool = False
    
    def __post_init__(self):
        """Calcula durações baseadas nas tasks."""
        self.duration_minutes = sum(task.estimate_minutes for task in self.tasks)
        # 6h efetivas por dia = 360 minutos
        self.duration_days = max(1, math.ceil(self.duration_minutes / 360))


class GitHubIntegration:
    """Gerencia integração com GitHub Issues e Milestones."""
    
    def __init__(self, github_repo: Optional[str] = None):
        self.github_repo = github_repo
        self.base_issue_url = f"https://github.com/{github_repo}/issues" if github_repo else None
        self.base_milestone_url = f"https://github.com/{github_repo}/milestone" if github_repo else None
        
    def generate_issue_url(self, epic_id: str) -> Optional[str]:
        """Gera URL da Issue baseada no ID do épico."""
        if not self.base_issue_url:
            return None
        # Mapear Epic ID para Issue number (customizável)
        issue_mapping = {
            "0": "1",
            "0.5": "2", 
            "2": "3",
            "3": "4",
            "4": "5",
            "5": "6",
            "6": "7",
            "7": "8",
            "8": "9"
        }
        issue_number = issue_mapping.get(epic_id)
        return f"{self.base_issue_url}/{issue_number}" if issue_number else None
        
    def generate_milestone_url(self, epic_id: str) -> Optional[str]:
        """Gera URL do Milestone baseada no ID do épico."""
        if not self.base_milestone_url:
            return None
        # Milestones principais
        milestone_mapping = {
            "0": "1",    # Foundation
            "3": "2",    # Core System  
            "8": "3"     # Analytics
        }
        milestone_number = milestone_mapping.get(epic_id)
        return f"{self.base_milestone_url}/{milestone_number}" if milestone_number else None
        
    def determine_epic_status(self, epic: Epic) -> str:
        """Determina status do épico baseado em heurísticas."""
        # Epic 0 como done (foundation), 0.5 como active
        if epic.id == "0":
            return "done"
        elif epic.id == "0.5":
            return "active"
        elif epic.id in ["2", "3", "5"]:
            return "crit"  # Critical path
        else:
            return "pending"
            
    def determine_task_status(self, task: Task, epic: Epic) -> str:
        """Determina status da task baseado em TDD phase e importância."""
        if task.estimate_minutes >= 30:  # Tasks longas são críticas
            return "crit"
        elif task.tdd_phase == "green":  # Implementation phase
            return "active"
        elif task.tdd_phase == "analysis" and any("milestone" in d.lower() for d in task.deliverables):
            return "milestone"
        else:
            return "pending"


class MermaidDiagramGenerator:
    """Gerador principal de diagramas Mermaid a partir dos épicos TDD."""
    
    def __init__(self, epics_dir: str = ".", github_repo: Optional[str] = None):
        self.epics_dir = Path(epics_dir)
        self.epics: List[Epic] = []
        self.github = GitHubIntegration(github_repo)
        self.epic_order = ["0", "0.5", "2", "3", "5", "6", "7", "4", "8"]  # Ordem heurística
        self.tdd_emojis = {
            "red": "🟥",
            "green": "🟩", 
            "refactor": "🟨",
            "analysis": "🟪"
        }
        # Professional status tags
        self.status_tags = {
            "done": "done",
            "active": "active", 
            "crit": "crit",
            "milestone": "milestone",
            "pending": ""
        }
    
    def sanitize_text(self, text: str) -> str:
        """Sanitiza texto para remover caracteres problemáticos no Mermaid."""
        if not text:
            return ""
        
        # Substituir caracteres UTF-8 problemáticos
        replacements = {
            "—": "-",       # Em dash
            "–": "-",       # En dash  
            """: '"',       # Left double quotation mark
            """: '"',       # Right double quotation mark
            "'": "'",       # Left single quotation mark
            "'": "'",       # Right single quotation mark
            "…": "...",     # Horizontal ellipsis
            "ã": "a",       # a com til
            "õ": "o",       # o com til
            "ç": "c",       # c cedilha
            "á": "a", "à": "a", "â": "a", "ä": "a",
            "é": "e", "è": "e", "ê": "e", "ë": "e", 
            "í": "i", "ì": "i", "î": "i", "ï": "i",
            "ó": "o", "ò": "o", "ô": "o", "ö": "o",
            "ú": "u", "ù": "u", "û": "u", "ü": "u"
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Remover caracteres não-ASCII restantes
        text = text.encode('ascii', errors='ignore').decode('ascii')
        
        return text
    
    def load_epics(self) -> None:
        """Carrega todos os arquivos epico_*.json encontrados."""
        epic_files = list(self.epics_dir.glob("epico_*.json"))
        epic_files.sort()  # Ordem alfabética
        
        print(f"📋 Encontrados {len(epic_files)} arquivos de épicos:")
        
        for epic_file in epic_files:
            try:
                with open(epic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                epic_data = data.get('epic', {})
                epic = Epic(
                    id=str(epic_data.get('id', '')),
                    name=epic_data.get('name', 'Epic sem nome')
                )
                
                # GitHub Integration para o épico
                epic.github_issue_url = self.github.generate_issue_url(epic.id)
                epic.github_milestone_url = self.github.generate_milestone_url(epic.id)
                epic.status = self.github.determine_epic_status(epic)
                epic.is_critical = epic.status == "crit"
                
                # Processar tasks
                for task_data in epic_data.get('tasks', []):
                    task = Task(
                        id=task_data.get('id', ''),
                        title=task_data.get('title', 'Task sem título'),
                        tdd_phase=task_data.get('tdd_phase', 'analysis'),
                        tdd_skip_reason=task_data.get('tdd_skip_reason'),
                        estimate_minutes=task_data.get('estimate_minutes', 10),
                        dependencies=task_data.get('dependencies', []),
                        deliverables=task_data.get('deliverables', [])
                    )
                    
                    # Criar global_id único
                    task.global_id = f"{epic.id}:{task.id}"
                    
                    # GitHub Integration para task
                    task.github_status = self.github.determine_task_status(task, epic)
                    task.is_critical = task.github_status == "crit"
                    
                    epic.tasks.append(task)
                
                self.epics.append(epic)
                print(f"   ✅ {epic_file.name}: Epic {epic.id} - {epic.name} ({len(epic.tasks)} tasks, {epic.duration_days}d)")
                
            except Exception as e:
                print(f"   ❌ Erro ao carregar {epic_file.name}: {e}")
    
    def get_critical_tasks(self, epic: Epic, top_k: int = 3) -> List[Task]:
        """Retorna as k tasks mais críticas de um épico (por estimate_minutes)."""
        if not epic.tasks:
            return []
        
        # Ordenar por estimate_minutes decrescente
        sorted_tasks = sorted(epic.tasks, key=lambda t: t.estimate_minutes, reverse=True)
        return sorted_tasks[:top_k]
    
    def generate_mindmap(self) -> str:
        """Gera o mindmap hierárquico com épicos e tasks."""
        lines = [
            "mindmap",
            "  root((🧠 ETL Debrito<br/>Sistema Interativo<br/>de Warnings))",
            "    📌 Épicos TDD"
        ]
        
        # Ordenar épicos pela ordem heurística
        sorted_epics = sorted(self.epics, key=lambda e: self.epic_order.index(e.id) if e.id in self.epic_order else 999)
        
        for epic in sorted_epics:
            # Título do épico com duração total
            epic_title = f"🎯 Epic {epic.id}: {epic.name}"
            lines.append(f"      {epic_title}")
            
            # Tasks do épico
            for task in epic.tasks:
                emoji = self.tdd_emojis.get(task.tdd_phase, "🟪")
                task_title = task.title[:50] + "..." if len(task.title) > 50 else task.title
                task_line = f"        {emoji} {task.id} — {task_title} ({task.estimate_minutes}min)"
                lines.append(task_line)
        
        return "\n".join(lines)
    
    def generate_flowchart(self) -> str:
        """Gera o flowchart com dependências entre épicos e tasks críticas."""
        lines = [
            "flowchart LR",
            "  classDef epic fill:#eef,stroke:#55f,stroke-width:2px,color:#000;",
            "  classDef task fill:#efe,stroke:#393,stroke-width:1px,color:#000;",
            "  classDef red fill:#fee,stroke:#f33,stroke-width:1px,color:#000;",  
            "  classDef green fill:#efe,stroke:#3f3,stroke-width:1px,color:#000;",
            "  classDef refactor fill:#ffd,stroke:#fa0,stroke-width:1px,color:#000;",
            ""
        ]
        
        # Criar nós dos épicos
        sorted_epics = sorted(self.epics, key=lambda e: self.epic_order.index(e.id) if e.id in self.epic_order else 999)
        
        epic_nodes = []
        for epic in sorted_epics:
            node_id = f"E{epic.id.replace('.', '_')}"
            node_label = f"Epic {epic.id}<br/>{epic.name}<br/>({epic.duration_days}d)"
            lines.append(f"  {node_id}[\"{node_label}\"]:::epic")
            epic_nodes.append(node_id)
        
        lines.append("")
        
        # Dependências entre épicos (ordem heurística)
        epic_flow = [
            ("E0", "E0_5"),
            ("E0_5", "E2"), 
            ("E2", "E3"),
            ("E3", "E5"),
            ("E3", "E4"),
            ("E2", "E6"),
            ("E5", "E7"),
            ("E5", "E8")
        ]
        
        for from_epic, to_epic in epic_flow:
            lines.append(f"  {from_epic} --> {to_epic}")
        
        lines.append("")
        
        # Tasks críticas de alguns épicos (exemplo com Epic 3)
        epic_3 = next((e for e in self.epics if e.id == "3"), None)
        if epic_3:
            critical_tasks = self.get_critical_tasks(epic_3, 3)[:3]  # Max 3 para não poluir
            task_nodes = []
            
            for i, task in enumerate(critical_tasks):
                node_id = f"T3_{i+1}"
                task_title = task.title[:25] + "..." if len(task.title) > 25 else task.title
                lines.append(f"  {node_id}[\"{task.id}<br/>{task_title}<br/>({task.estimate_minutes}min)\"]:::{task.tdd_phase}")
                task_nodes.append(node_id)
            
            # Conectar Epic 3 com suas tasks críticas
            if task_nodes:
                lines.append(f"  E3 --> {task_nodes[0]}")
                
                # Chain das tasks (se houver sequência red→green→refactor)
                for j in range(len(task_nodes) - 1):
                    lines.append(f"  {task_nodes[j]} --> {task_nodes[j+1]}")
        
        return "\n".join(lines)
    
    def calculate_start_date(self, epic_id: str) -> str:
        """Calcula data de início baseada na ordem e dependências."""
        base_date = datetime(2025, 8, 11)  # Data base (segunda-feira)
        
        if epic_id == "0":
            return base_date.strftime("%Y-%m-%d")
        
        # Acumular dias dos épicos anteriores na ordem
        accumulated_days = 0
        for order_id in self.epic_order:
            if order_id == epic_id:
                break
            epic = next((e for e in self.epics if e.id == order_id), None)
            if epic:
                accumulated_days += epic.duration_days
        
        start_date = base_date + timedelta(days=accumulated_days)
        return start_date.strftime("%Y-%m-%d")
    
    def generate_gantt(self) -> str:
        """Gera o cronograma Gantt com durações e dependências."""
        lines = [
            "gantt",
            "  title ETL Debrito - Cronograma TDD Profissional",
            "  dateFormat YYYY-MM-DD",
            "  axisFormat %d/%m",
            "  excludes weekends",
            ""
        ]
        
        # Organizar por seções lógicas (ASCII safe)
        sections = {
            "Fundacoes": ["0", "0.5"],
            "Nucleo": ["2", "3", "5", "7"], 
            "Dados e Produtividade": ["6", "4"],
            "Observabilidade": ["8"]
        }
        
        for section_name, epic_ids in sections.items():
            lines.append(f"  section {section_name}")
            
            for epic_id in epic_ids:
                epic = next((e for e in self.epics if e.id == epic_id), None)
                if not epic:
                    continue
                
                # Determinar dependência (after)
                dependencies = self._get_epic_dependencies(epic_id)
                after_clause = f", after {dependencies}" if dependencies else ""
                
                # Status profissional baseado na integração GitHub
                status_tag = self.status_tags.get(epic.status, "")
                if status_tag:
                    status = f"{status_tag}, "
                    # Adicionar tags adicionais para épicos críticos
                    if epic.is_critical and status_tag != "crit":
                        status = f"crit, {status}"
                else:
                    status = "        "
                
                # Calcular data de início
                start_date = self.calculate_start_date(epic_id)
                
                # Sanitizar nome do épico
                clean_name = self.sanitize_text(epic.name)
                epic_line = f"  Epic {epic.id}: {clean_name[:30]}..."
                if len(clean_name) <= 30:
                    epic_line = f"  Epic {epic.id}: {clean_name}"
                
                gantt_id = f"e{epic.id.replace('.', '_')}"
                duration_spec = f"{epic.duration_days}d" if not after_clause else f"{epic.duration_days}d"
                
                # Adicionar épico com sintaxe limpa e indentação consistente (2 espaços)
                formatted_epic = f"Epic {epic.id}: {clean_name}"
                lines.append(f"  {formatted_epic:<45} :{status}{gantt_id}, {start_date}, {duration_spec}")
                
                # Não adicionar milestones inline - serão adicionados na seção dedicada
                pass
            
            lines.append("")
        
        # Adicionar seção de marcos importantes
        lines.append("")
        lines.append("  section Marcos")
        lines.append("  Foundation Complete                         :milestone, foundation, 2025-08-12, 0d")
        lines.append("  Core System Ready                          :milestone, core_ready, 2025-08-16, 0d")
        lines.append("  Analytics Live                             :milestone, analytics, 2025-08-19, 0d")
        
        return "\n".join(lines)
    
    def _get_epic_dependencies(self, epic_id: str) -> str:
        """Retorna string de dependência para Gantt (ex: 'e0' ou 'e0_5')."""
        dependency_map = {
            "0.5": "e0",
            "2": "e0_5", 
            "3": "e2",
            "5": "e3",
            "6": "e2",
            "7": "e5",
            "4": "e3",
            "8": "e5"
        }
        return dependency_map.get(epic_id, "")
    
    def generate_all_diagrams(self) -> Tuple[str, str, str]:
        """Gera todos os três diagramas e retorna como tupla."""
        mindmap = self.generate_mindmap()
        flowchart = self.generate_flowchart() 
        gantt = self.generate_gantt()
        
        return mindmap, flowchart, gantt
    
    def save_diagrams(self, output_dir: str = "docs") -> None:
        """Salva todos os diagramas em arquivos .mmd separados."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        mindmap, flowchart, gantt = self.generate_all_diagrams()
        
        # Salvar arquivos
        files = [
            ("mindmap.mmd", mindmap),
            ("flow_dependencies.mmd", flowchart),
            ("gantt_schedule.mmd", gantt)
        ]
        
        for filename, content in files:
            file_path = output_path / filename
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"💾 Salvo: {file_path}")
    
    def print_summary(self) -> None:
        """Imprime resumo dos épicos processados."""
        total_tasks = sum(len(epic.tasks) for epic in self.epics)
        total_minutes = sum(epic.duration_minutes for epic in self.epics)
        total_days = sum(epic.duration_days for epic in self.epics)
        
        print(f"\n📊 Resumo:")
        print(f"   Épicos: {len(self.epics)}")
        print(f"   Tasks total: {total_tasks}")
        print(f"   Tempo total: {total_minutes} minutos ({total_days} dias)")
        
        print(f"\n📋 Por épico:")
        for epic in sorted(self.epics, key=lambda e: self.epic_order.index(e.id) if e.id in self.epic_order else 999):
            print(f"   Epic {epic.id}: {len(epic.tasks)} tasks, {epic.duration_minutes}min ({epic.duration_days}d)")


def main():
    """Função principal com suporte a argumentos GitHub."""
    parser = argparse.ArgumentParser(
        description="🧠 ETL Debrito - Gerador de Diagramas Mermaid TDD com Integração GitHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python generate_all_diagrams.py
  python generate_all_diagrams.py --github-repo davidcantidio/etl_debrito
  python generate_all_diagrams.py --github-repo owner/repo --output-dir docs
        """
    )
    
    parser.add_argument(
        "--github-repo", 
        type=str, 
        help="Repositório GitHub no formato 'owner/repo' para integração com Issues/Milestones"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="docs",
        help="Diretório de saída para os arquivos .mmd (padrão: docs)"
    )
    
    args = parser.parse_args()
    
    print("🧠 ETL Debrito - Gerador de Diagramas Mermaid TDD com GitHub Integration")
    print("=" * 70)
    
    if args.github_repo:
        print(f"🔗 GitHub Repo: {args.github_repo}")
        print(f"📁 Output Dir: {args.output_dir}")
    
    # Verificar se estamos no diretório correto
    if not Path("epico_0.json").exists():
        print("❌ Erro: Execute no diretório contendo os arquivos epico_*.json")
        return 1
    
    # Inicializar gerador com integração GitHub
    generator = MermaidDiagramGenerator(github_repo=args.github_repo)
    
    try:
        # Carregar épicos
        generator.load_epics()
        
        if not generator.epics:
            print("❌ Nenhum épico encontrado!")
            return 1
        
        # Gerar diagramas
        print(f"\n🎨 Gerando diagramas...")
        generator.save_diagrams(output_dir=args.output_dir)
        
        # Mostrar resumo
        generator.print_summary()
        
        print(f"\n✅ Diagramas gerados com sucesso!")
        print(f"   📁 Veja os arquivos em: {args.output_dir}/")
        print(f"   🌐 Visualize em: https://mermaid.live/")
        
        if args.github_repo:
            print(f"\n🔗 Recursos GitHub Integration:")
            print(f"   • Links interativos para Issues do repositório")
            print(f"   • Status tags profissionais (done, active, crit)")
            print(f"   • Milestones conectados ao GitHub")
            print(f"   • Vertical markers para datas importantes")
        
        return 0
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())