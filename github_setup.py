#!/usr/bin/env python3
"""
🎯 GitHub Setup - ETL Debrito Project Organization
=================================================

Script para automatizar a criação de Milestones e Issues
baseado nos épicos JSON existentes do projeto ETL Debrito.

Funcionalidades:
- Criação automática de Milestones para cada épico
- Geração de Issues usando template epic.yml
- Aplicação de labels consistentes
- Setup de Project Board

Uso:
    python github_setup.py --create-milestones
    python github_setup.py --create-issues  
    python github_setup.py --setup-all

Requires: gh CLI configurado com autenticação
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional


class GitHubSetup:
    """Classe para automatizar setup do GitHub para o projeto ETL Debrito."""
    
    def __init__(self, repo: str = "davidcantidio/etl_debrito"):
        self.repo = repo
        self.epics_data = {}
        self.base_date = datetime.now()
        
    def load_epics(self) -> Dict:
        """Carrega todos os épicos JSON do diretório."""
        epic_files = list(Path('.').glob('epico_*.json'))
        
        if not epic_files:
            print("❌ Nenhum arquivo epico_*.json encontrado")
            return {}
        
        epics = {}
        for epic_file in epic_files:
            try:
                with open(epic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    epic_id = data['epic']['id']
                    epics[epic_id] = data['epic']
                    print(f"✅ Carregado: {epic_file.name} (Epic {epic_id})")
            except Exception as e:
                print(f"❌ Erro ao carregar {epic_file}: {e}")
        
        self.epics_data = epics
        return epics
    
    def create_milestones(self) -> None:
        """Cria milestones no GitHub para cada épico."""
        print("🎯 Criando Milestones no GitHub...")
        
        for epic_id, epic in self.epics_data.items():
            title = f"EPIC {epic_id} - {epic['name']}"
            description = f"{epic['summary']}\n\n📊 Duration: {epic.get('duration', 'TBD')}"
            
            # Calcular due date baseado na duração
            duration_days = self._parse_duration(epic.get('duration', '1 dia'))
            epic_multiplier = self._parse_epic_id(epic_id)
            due_date = (self.base_date + timedelta(days=duration_days * epic_multiplier)).strftime('%Y-%m-%d')
            
            cmd = [
                'gh', 'api', f'repos/{self.repo}/milestones',
                '--method', 'POST',
                '--field', f'title={title}',
                '--field', f'description={description}',
                '--field', f'due_on={due_date}T23:59:59Z',
                '--field', 'state=open'
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Milestone criado: {title}")
            except subprocess.CalledProcessError as e:
                if "already_exists" in e.stderr or "Validation Failed" in e.stderr:
                    print(f"⚠️ Milestone já existe: {title}")
                else:
                    print(f"❌ Erro ao criar milestone {title}: {e.stderr}")
    
    def create_issues(self) -> None:
        """Cria Issues no GitHub para cada épico usando template."""
        print("📝 Criando Issues no GitHub...")
        
        for epic_id, epic in self.epics_data.items():
            title = f"[Epic {epic_id}] {epic['name']}"
            
            # Criar corpo da issue usando template structure
            body = self._generate_issue_body(epic_id, epic)
            
            # Definir labels baseadas no épico
            labels = self._get_epic_labels(epic)
            labels_str = ','.join(labels)
            
            cmd = [
                'gh', 'issue', 'create',
                '--repo', self.repo,
                '--title', title,
                '--body', body,
                '--label', labels_str
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                issue_url = result.stdout.strip()
                print(f"✅ Issue criada: {title}")
                print(f"   🔗 {issue_url}")
            except subprocess.CalledProcessError as e:
                print(f"❌ Erro ao criar issue {title}: {e.stderr}")
    
    def setup_labels(self) -> None:
        """Configura labels padrão do projeto."""
        print("🏷️ Configurando labels do projeto...")
        
        labels = [
            {'name': 'epic', 'color': '0366d6', 'description': 'Epic implementation issue'},
            {'name': 'tdd', 'color': 'e99695', 'description': 'Test-Driven Development'},
            {'name': 'red', 'color': 'd73a4a', 'description': 'TDD Red Phase - Tests failing'},
            {'name': 'green', 'color': '28a745', 'description': 'TDD Green Phase - Implementation'},
            {'name': 'refactor', 'color': 'fbca04', 'description': 'TDD Refactor Phase - Code improvement'},
            {'name': 'analysis', 'color': '6f42c1', 'description': 'Analysis and documentation tasks'},
            {'name': 'tdah', 'color': 'ff69b4', 'description': 'TDAH-optimized workflow tools'},
            {'name': 'performance', 'color': 'ff6347', 'description': 'Performance optimization'},
            {'name': 'compatibility', 'color': '17a2b8', 'description': 'System compatibility'},
            {'name': 'integration', 'color': '20c997', 'description': 'System integration tasks'},
            # Labels específicas dos épicos
            {'name': 'caching', 'color': 'ffd700', 'description': 'Cache management and optimization'},
            {'name': 'env', 'color': '32cd32', 'description': 'Environment and production safety'},
            {'name': 'productivity', 'color': 'ff8c00', 'description': 'Productivity and time tracking'},
            {'name': 'interactive-system', 'color': '9370db', 'description': 'Interactive warning resolution system'},
            {'name': 'logging', 'color': '4682b4', 'description': 'Logging and compatibility analysis'},
            {'name': 'cli', 'color': '8fbc8f', 'description': 'Command line interface tools'},
            {'name': 'architecture', 'color': '2e8b57', 'description': 'Architecture and integration fixes'},
            {'name': 'migration', 'color': 'dc143c', 'description': 'Data migration and issues integration'},
            {'name': 'time-tracking', 'color': 'ff1493', 'description': 'Task time monitoring and analytics'},
            {'name': 'safety', 'color': 'b22222', 'description': 'Production safety and environment'},
            {'name': 'discovery', 'color': '4169e1', 'description': 'Discovery and compatibility analysis'},
            {'name': 'tooling', 'color': '228b22', 'description': 'Development tooling and utilities'},
            {'name': 'analytics', 'color': '800080', 'description': 'Analytics and monitoring'},
            {'name': 'write-back', 'color': '008b8b', 'description': 'Write-back optimization'},
            {'name': 'warnings', 'color': 'ff4500', 'description': 'Warning system and resolution'},
            {'name': 'refactoring', 'color': 'daa520', 'description': 'Code refactoring and improvement'},
            {'name': 'github-integration', 'color': '191970', 'description': 'GitHub integration features'}
        ]
        
        for label in labels:
            cmd = [
                'gh', 'api', f'repos/{self.repo}/labels',
                '--method', 'POST',
                '--field', f'name={label["name"]}',
                '--field', f'color={label["color"]}',
                '--field', f'description={label["description"]}'
            ]
            
            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f"✅ Label criada: {label['name']}")
            except subprocess.CalledProcessError as e:
                if "already_exists" in e.stderr:
                    print(f"⚠️ Label já existe: {label['name']}")
                else:
                    print(f"❌ Erro ao criar label {label['name']}: {e.stderr}")
    
    def _parse_duration(self, duration_str: str) -> int:
        """Parse duration string para dias."""
        if 'dia' in duration_str.lower():
            numbers = [int(s) for s in duration_str.split() if s.isdigit()]
            return numbers[0] if numbers else 1
        return 1
    
    def _parse_epic_id(self, epic_id: str) -> int:
        """Parse epic ID para multiplicador de data."""
        try:
            if '.' in epic_id:
                # Para épicos como 0.5, usar parte inteira + 1
                return int(float(epic_id)) + 1
            else:
                return int(epic_id) + 1
        except:
            return 1
    
    def _generate_issue_body(self, epic_id: str, epic: Dict) -> str:
        """Gera corpo da issue baseado no template epic.yml."""
        
        # Extrair tasks summary
        tasks = epic.get('tasks', [])[:5]  # Mostrar apenas primeiras 5 tasks
        tasks_summary = "\n".join([
            f"- [ ] **Task {task.get('id', 'X')}**: {task.get('title', task.get('description', 'No title'))[:80]}"
            for task in tasks
        ])
        
        if len(epic.get('tasks', [])) > 5:
            tasks_summary += f"\n- ... e mais {len(epic.get('tasks', [])) - 5} tasks"
        
        # Status TDD
        tdd_status = "📋 ANALYSIS Phase: Análise e documentação completa"
        if epic.get('tdd_enabled'):
            tdd_status = """🔴 RED Phase: Testes falhando escritos
🟢 GREEN Phase: Implementação para passar os testes  
🟨 REFACTOR Phase: Otimização e limpeza do código"""
        
        body = f"""## 🧠 ETL Debrito - Epic Implementation Issue

**Epic ID:** {epic_id}
**Epic Name:** {epic['name']}
**Status:** pending - Não iniciado
**Duration:** {epic.get('duration', 'TBD')}

## Epic Overview
{epic['summary']}

### Goals
{chr(10).join([f"- {goal}" for goal in epic.get('goals', [])])}

### Definition of Done  
{chr(10).join([f"- [ ] {done}" for done in epic.get('definition_of_done', [])])}

## Tasks Summary
{tasks_summary}

## TDD Implementation Status
{tdd_status}

## Acceptance Criteria
- [ ] Todos os testes TDD passando
- [ ] Cobertura de código ≥ 90%  
- [ ] Performance requirements atendidos
- [ ] Testes de integração passando
- [ ] Documentação atualizada
- [ ] Code review aprovado

## 🔗 Links Automáticos

Esta Issue está automaticamente conectada ao sistema de Gantt charts do projeto:

- 📊 **[Gantt Timeline](../blob/main/docs/gantt_schedule.mmd)** - Timeline interativo
- 🧠 **[Project Mindmap](../blob/main/docs/mindmap.mmd)** - Estrutura hierárquica  
- 🔄 **[Dependencies Flow](../blob/main/docs/flow_dependencies.mmd)** - Mapa de dependências

**Visualização interativa:** Copie o conteúdo dos arquivos .mmd para [mermaid.live](https://mermaid.live/)

---
🎯 Auto-generated Epic Issue - ETL Debrito Project"""
        
        return body
    
    def _get_epic_labels(self, epic: Dict) -> List[str]:
        """Define labels baseadas no épico - simplificado para funcionar."""
        labels = ['epic']
        
        if epic.get('tdd_enabled'):
            labels.append('tdd')
        
        # Labels seguras que existem
        safe_labels = ['analysis', 'performance', 'compatibility', 'integration', 
                      'caching', 'env', 'safety', 'discovery', 'tooling']
        
        # Adicionar apenas labels que sabemos que existem
        epic_labels = epic.get('labels', [])
        for label in epic_labels:
            if label in safe_labels and label not in labels:
                labels.append(label)
        
        return labels
    
    def _get_milestone_number(self, milestone_title: str) -> Optional[int]:
        """Obter número do milestone pelo título."""
        try:
            cmd = ['gh', 'api', f'repos/{self.repo}/milestones?state=all']
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            milestones = json.loads(result.stdout)
            
            for milestone in milestones:
                if milestone['title'] == milestone_title:
                    return milestone['number']
        except Exception as e:
            print(f"⚠️ Erro ao buscar milestone '{milestone_title}': {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Setup GitHub organization for ETL Debrito project')
    parser.add_argument('--create-milestones', action='store_true', help='Create milestones for epics')
    parser.add_argument('--create-issues', action='store_true', help='Create issues for epics')
    parser.add_argument('--setup-labels', action='store_true', help='Setup project labels')
    parser.add_argument('--setup-all', action='store_true', help='Run complete setup')
    parser.add_argument('--repo', default='davidcantidio/etl_debrito', help='GitHub repository')
    
    args = parser.parse_args()
    
    # Verificar se gh CLI está disponível
    try:
        subprocess.run(['gh', '--version'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ GitHub CLI (gh) não encontrado. Instale: https://cli.github.com/")
        sys.exit(1)
    
    # Inicializar setup
    setup = GitHubSetup(args.repo)
    
    # Carregar épicos
    epics = setup.load_epics()
    if not epics:
        print("❌ Nenhum épico carregado. Verifique os arquivos epico_*.json")
        sys.exit(1)
    
    print(f"📊 Carregados {len(epics)} épicos: {list(epics.keys())}")
    
    # Executar actions baseadas nos argumentos
    if args.setup_all or args.setup_labels:
        setup.setup_labels()
    
    if args.setup_all or args.create_milestones:
        setup.create_milestones()
    
    if args.setup_all or args.create_issues:
        setup.create_issues()
    
    if args.setup_all:
        print("\n🎉 Setup completo do GitHub realizado!")
        print("📊 Próximos passos:")
        print("  1. Verificar Milestones criados no GitHub")
        print("  2. Revisar Issues geradas")
        print("  3. Configurar Project Board")
        print("  4. Executar workflow Gantt charts")
    
    if not any([args.create_milestones, args.create_issues, args.setup_labels, args.setup_all]):
        parser.print_help()


if __name__ == '__main__':
    main()